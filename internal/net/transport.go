package net

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/liukeqqs/core/common/bufpool"
)

const (
	bufferSize        = 64 * 1024
	maxQueueSize      = 100000      // 队列最大容量
	retryInterval     = 100 * time.Millisecond // 重试间隔
	flushInterval     = 500 * time.Millisecond // 批量上报间隔
	maxBatchSize      = 100          // 批量上报最大条数
	reportChannelSize = 10000        // 流量报告通道大小
	maxRetries        = 10           // 最大重试次数
	// 新增配置
	diskBufferDir     = "./traffic_buffer" // 磁盘缓冲区目录
	diskBufferSize    = 100 * 1024 * 1024  // 磁盘缓冲区大小，100MB
	statsMonitorInterval = 5 * time.Second // 统计监控间隔
)

var (
	RChan       = make(chan Info, 5120)
	rchanQueue  = make(chan Info, maxQueueSize)
	queueOnce   sync.Once
	reportChan  = make(chan Info, reportChannelSize) // 流量报告通道
	// 新增磁盘缓冲区和状态跟踪
	diskBuffer      = NewDiskBuffer(diskBufferDir, diskBufferSize)
	statsProcessed  int64 // 已处理的统计信息总数
	statsDropped    int64 // 丢弃的统计信息总数
	statsRetried    int64 // 重试的统计信息总数
	statsFromDisk   int64 // 从磁盘恢复的统计信息总数
	queueSize       int64 // 队列当前大小
	queueTraffic    int64 // 队列中的流量总和
)

// Info 增强版流量统计结构
type Info struct {
	Address    string `json:"address"`
	LocalPort  int    `json:"localport"`
	Bytes      int64  `json:"bytes"`
	Unix       int64  `json:"unix"`
	RepeatNums int64  `json:"repeatnums"`
	SessionID  string `json:"sid,omitempty"`   // 会话ID
	Domain     string `json:"domain,omitempty"`// 域名
	Retries    int    `json:"retries,omitempty"` // 重试次数
	// 新增字段
	ID         string `json:"id,omitempty"`    // 唯一ID
	Timestamp  int64  `json:"timestamp"`       // 时间戳
	FromDisk   bool   `json:"from_disk,omitempty"` // 是否从磁盘恢复
}

// 初始化后台队列处理器
func init() {
	queueOnce.Do(func() {
		go processRChanQueue()
		go batchReportStats()
		go monitorStats() // 监控统计信息处理情况
		go monitorQueueStatus() // 监控队列状态
	})
}

// 监控统计信息处理情况
func monitorStats() {
	ticker := time.NewTicker(statsMonitorInterval)
	defer ticker.Stop()

	lastProcessed := int64(0)
	lastDropped := int64(0)
	lastRetried := int64(0)
	lastFromDisk := int64(0)

	for range ticker.C {
		currentProcessed := atomic.LoadInt64(&statsProcessed)
		currentDropped := atomic.LoadInt64(&statsDropped)
		currentRetried := atomic.LoadInt64(&statsRetried)
		currentFromDisk := atomic.LoadInt64(&statsFromDisk)
		currentQueueSize := atomic.LoadInt64(&queueSize)
		currentQueueTraffic := atomic.LoadInt64(&queueTraffic)

		log.Printf("[流量统计监控] 处理: %d/s | 丢弃: %d/s | 重试: %d/s | 磁盘恢复: %d/s | 队列: %d条/%d字节",
			(currentProcessed-lastProcessed)/int64(statsMonitorInterval/time.Second),
			(currentDropped-lastDropped)/int64(statsMonitorInterval/time.Second),
			(currentRetried-lastRetried)/int64(statsMonitorInterval/time.Second),
			(currentFromDisk-lastFromDisk)/int64(statsMonitorInterval/time.Second),
			currentQueueSize, currentQueueTraffic)

		lastProcessed = currentProcessed
		lastDropped = currentDropped
		lastRetried = currentRetried
		lastFromDisk = currentFromDisk
	}
}

// 监控队列状态
func monitorQueueStatus() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		log.Printf("[队列状态] reportChan: %d/%d | rchanQueue: %d/%d | 待上传流量: %d字节",
			len(reportChan), cap(reportChan),
			len(rchanQueue), cap(rchanQueue),
			atomic.LoadInt64(&queueTraffic))
	}
}

// 可靠队列处理核心逻辑
func processRChanQueue() {
	for info := range rchanQueue {
		// 更新队列状态
		atomic.AddInt64(&queueSize, -1)
		atomic.AddInt64(&queueTraffic, -info.Bytes)

		// 指数退避重试机制
		success := false
		for attempt := 0; attempt < maxRetries; attempt++ {
			select {
			case RChan <- info:
				success = true
				atomic.AddInt64(&statsProcessed, 1)
				break
			default:
				// 队列满，等待后重试
				delay := time.Duration(1<<uint(attempt)) * retryInterval
				log.Printf("RChan队列阻塞，第 %d 次重试，等待 %v: %v", attempt+1, delay, info)
				time.Sleep(delay)
				atomic.AddInt64(&statsRetried, 1)
			}

			if success {
				break
			}
		}

		if !success {
			// 所有重试都失败，写入磁盘缓冲区
			log.Printf("警告: 流量统计信息内存队列已满，写入磁盘缓冲区: %v", info)
			if err := diskBuffer.Write(info); err != nil {
				log.Printf("错误: 写入磁盘缓冲区失败: %v", err)
				atomic.AddInt64(&statsDropped, 1)
			} else {
				atomic.AddInt64(&statsFromDisk, 1)
			}
		}
	}
}

// 批量上报流量统计
func batchReportStats() {
	batch := make([]Info, 0, maxBatchSize)
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	for {
		select {
		case info := <-reportChan:
			// 更新队列状态
			atomic.AddInt64(&queueSize, -1)
			atomic.AddInt64(&queueTraffic, -info.Bytes)

			batch = append(batch, info)
			if len(batch) >= maxBatchSize {
				// 达到批量大小，立即处理
				if !flushBatchWithRetry(batch) {
					// 如果批量处理失败，将数据写入磁盘缓冲区
					for _, item := range batch {
						if err := diskBuffer.Write(item); err != nil {
							log.Printf("错误: 写入磁盘缓冲区失败: %v", err)
							atomic.AddInt64(&statsDropped, 1)
						} else {
							atomic.AddInt64(&statsFromDisk, 1)
						}
					}
				}
				batch = make([]Info, 0, maxBatchSize)
			}
		case <-ticker.C:
			// 时间间隔到，处理当前批次
			if len(batch) > 0 {
				if !flushBatchWithRetry(batch) {
					// 如果批量处理失败，将数据写入磁盘缓冲区
					for _, item := range batch {
						if err := diskBuffer.Write(item); err != nil {
							log.Printf("错误: 写入磁盘缓冲区失败: %v", err)
							atomic.AddInt64(&statsDropped, 1)
						} else {
							atomic.AddInt64(&statsFromDisk, 1)
						}
					}
				}
				batch = make([]Info, 0, maxBatchSize)
			}

			// 尝试从磁盘缓冲区恢复数据
			recoverFromDisk()
		}
	}
}

// 从磁盘缓冲区恢复数据
func recoverFromDisk() {
	items, err := diskBuffer.ReadBatch(maxBatchSize)
	if err != nil {
		log.Printf("错误: 从磁盘缓冲区读取数据失败: %v", err)
		return
	}

	for _, item := range items {
		// 标记为从磁盘恢复
		item.FromDisk = true

		// 更新队列状态
		atomic.AddInt64(&queueSize, 1)
		atomic.AddInt64(&queueTraffic, item.Bytes)

		// 非阻塞方式发送
		select {
		case reportChan <- item:
			// 发送成功
		default:
			// 内存队列已满，继续保留在磁盘
			log.Printf("警告: 内存队列已满，保留在磁盘缓冲区: %v", item)
			if err := diskBuffer.Write(item); err != nil {
				log.Printf("错误: 写回磁盘缓冲区失败: %v", err)
			}
		}
	}
}

// 带重试的批量处理
func flushBatchWithRetry(batch []Info) bool {
	maxRetries := 5
	for attempt := 0; attempt < maxRetries; attempt++ {
		success := true
		for _, info := range batch {
			select {
			case rchanQueue <- info:
				// 更新队列状态
				atomic.AddInt64(&queueSize, 1)
				atomic.AddInt64(&queueTraffic, info.Bytes)
				// 发送成功
			default:
				// 队列满，尝试失败
				success = false
				break
			}
		}

		if success {
			return true
		}

		// 指数退避
		delay := time.Duration(1<<uint(attempt)) * retryInterval
		log.Printf("批量处理失败，第 %d 次重试，等待 %v", attempt+1, delay)
		time.Sleep(delay)
	}

	return false
}

// 可靠上报流量统计
func reportStats(info Info) {
	// 确保信息有唯一ID和时间戳
	if info.ID == "" {
		info.ID = generateUniqueID()
	}
	if info.Timestamp == 0 {
		info.Timestamp = time.Now().UnixNano()
	}

	// 更新队列状态
	atomic.AddInt64(&queueSize, 1)
	atomic.AddInt64(&queueTraffic, info.Bytes)

	// 可靠发送，不允许丢失
	for attempt := 0; attempt < maxRetries; attempt++ {
		select {
		case reportChan <- info:
			// 发送成功
			return
		default:
			// 队列满，写入磁盘缓冲区
			log.Printf("警告: 内存队列已满，写入磁盘缓冲区: %v", info)
			if err := diskBuffer.Write(info); err != nil {
				log.Printf("错误: 写入磁盘缓冲区失败: %v", err)
				// 继续重试
			} else {
				atomic.AddInt64(&statsFromDisk, 1)
				return
			}

			// 短暂等待后重试
			time.Sleep(retryInterval)
		}
	}

	// 所有重试都失败，这是最后的保底措施
	log.Printf("严重错误: 流量统计信息无法发送或写入磁盘: %v", info)
	atomic.AddInt64(&statsDropped, 1)
}

// 生成唯一ID
func generateUniqueID() string {
	return time.Now().Format("20060102150405.000000") + "-" + string(atomic.AddInt64(&statsProcessed, 1))
}

// 磁盘缓冲区接口
type DiskBuffer interface {
	Write(info Info) error
	ReadBatch(maxSize int) ([]Info, error)
	Size() int64
	Cleanup() error
}

// 简单的磁盘缓冲区实现
type diskBuffer struct {
	dir       string
	maxSize   int64
	currentSize int64
	mutex     sync.RWMutex
}

// 创建新的磁盘缓冲区
func NewDiskBuffer(dir string, maxSize int64) DiskBuffer {
	// 确保目录存在
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Printf("错误: 创建磁盘缓冲区目录失败: %v", err)
	}

	return &diskBuffer{
		dir:     dir,
		maxSize: maxSize,
	}
}

// 写入统计信息到磁盘
func (db *diskBuffer) Write(info Info) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	// 检查磁盘缓冲区大小
	if db.currentSize >= db.maxSize {
		// 缓冲区已满，尝试清理
		if err := db.cleanupOldest(); err != nil {
			return err
		}
	}

	// 序列化为JSON
	data, err := json.Marshal(info)
	if err != nil {
		return err
	}

	// 写入文件
	fileName := fmt.Sprintf("%s/%s.json", db.dir, info.ID)
	if err := ioutil.WriteFile(fileName, data, 0644); err != nil {
		return err
	}

	// 更新当前大小
	db.currentSize += int64(len(data))
	return nil
}

// 从磁盘读取一批统计信息
func (db *diskBuffer) ReadBatch(maxSize int) ([]Info, error) {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	// 读取目录中的所有文件
	files, err := ioutil.ReadDir(db.dir)
	if err != nil {
		return nil, err
	}

	// 按时间排序
	sort.Slice(files, func(i, j int) bool {
		return files[i].ModTime().Before(files[j].ModTime())
	})

	// 读取最多maxSize个文件
	var items []Info
	var totalSize int64

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		// 读取文件内容
		data, err := ioutil.ReadFile(filepath.Join(db.dir, file.Name()))
		if err != nil {
			log.Printf("警告: 读取磁盘缓冲区文件失败: %v", err)
			continue
		}

		// 解析JSON
		var info Info
		if err := json.Unmarshal(data, &info); err != nil {
			log.Printf("警告: 解析磁盘缓冲区数据失败: %v", err)
			continue
		}

		items = append(items, info)
		totalSize += int64(len(data))

		// 删除文件
		if err := os.Remove(filepath.Join(db.dir, file.Name())); err != nil {
			log.Printf("警告: 删除磁盘缓冲区文件失败: %v", err)
		} else {
			db.currentSize -= int64(len(data))
		}

		// 达到最大数量
		if len(items) >= maxSize {
			break
		}
	}

	return items, nil
}

// 获取当前磁盘缓冲区大小
func (db *diskBuffer) Size() int64 {
	db.mutex.RLock()
	defer db.mutex.RUnlock()
	return db.currentSize
}

// 清理磁盘缓冲区
func (db *diskBuffer) Cleanup() error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	// 删除所有文件
	files, err := ioutil.ReadDir(db.dir)
	if err != nil {
		return err
	}

	for _, file := range files {
		if !file.IsDir() {
			if err := os.Remove(filepath.Join(db.dir, file.Name())); err != nil {
				log.Printf("警告: 清理磁盘缓冲区文件失败: %v", err)
			}
		}
	}

	db.currentSize = 0
	return nil
}

// 清理最旧的文件
func (db *diskBuffer) cleanupOldest() error {
	// 读取目录中的所有文件
	files, err := ioutil.ReadDir(db.dir)
	if err != nil {
		return err
	}

	// 按时间排序
	sort.Slice(files, func(i, j int) bool {
		return files[i].ModTime().Before(files[j].ModTime())
	})

	// 删除最旧的文件
	if len(files) > 0 {
		filePath := filepath.Join(db.dir, files[0].Name())
		data, err := ioutil.ReadFile(filePath)
		if err != nil {
			return err
		}

		if err := os.Remove(filePath); err != nil {
			return err
		}

		db.currentSize -= int64(len(data))
	}

	return nil
}

// 流量统计包装器
type TrafficCounter struct {
	reader     io.Reader
	writer     io.Writer
	bytesRead  int64
	bytesWrite int64
	// 新增字段
	sid       string
	address   string
	domain    string
	localPort int
	startTime time.Time
	stopChan  chan struct{}
	wg        sync.WaitGroup
}

// 创建新的流量计数器
func NewTrafficCounter(r io.Reader, w io.Writer, sid, address, domain string, localPort int) *TrafficCounter {
	return &TrafficCounter{
		reader:    r,
		writer:    w,
		startTime: time.Now(),
		sid:       sid,
		address:   address,
		domain:    domain,
		localPort: localPort,
		stopChan:  make(chan struct{}),
	}
}

// 实现Reader接口
func (tc *TrafficCounter) Read(p []byte) (n int, err error) {
	n, err = tc.reader.Read(p)
	if n > 0 {
		atomic.AddInt64(&tc.bytesRead, int64(n))
	}
	return
}

// 实现Writer接口
func (tc *TrafficCounter) Write(p []byte) (n int, err error) {
	n, err = tc.writer.Write(p)
	if n > 0 {
		atomic.AddInt64(&tc.bytesWrite, int64(n))
	}
	return
}

// 获取读取的字节数
func (tc *TrafficCounter) BytesRead() int64 {
	return atomic.LoadInt64(&tc.bytesRead)
}

// 获取写入的字节数
func (tc *TrafficCounter) BytesWritten() int64 {
	return atomic.LoadInt64(&tc.bytesWrite)
}

// 定期上报流量统计
func (tc *TrafficCounter) StartPeriodicReport(interval time.Duration) {
	tc.wg.Add(1)
	go func() {
		defer tc.wg.Done()

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		var lastRead, lastWrite int64

		for {
			select {
			case <-ticker.C:
				currentRead := tc.BytesRead()
				currentWrite := tc.BytesWritten()

				// 只在有流量变化时上报
				if currentRead > lastRead || currentWrite > lastWrite {
					info := Info{
						Address:    tc.address,
						LocalPort:  tc.localPort,
						Bytes:      currentRead + currentWrite,
						Unix:       time.Now().Unix(),
						RepeatNums: 1,
						SessionID:  tc.sid,
						Domain:     tc.domain,
						ID:         generateUniqueID(),
						Timestamp:  time.Now().UnixNano(),
					}

					reportStats(info)
					log.Printf("[定期流量] SessionID=%s | 累计上行=%d | 累计下行=%d | 累计总流量=%d | 耗时=%v",
						tc.sid, currentWrite, currentRead, currentRead+currentWrite, time.Since(tc.startTime))

					lastRead = currentRead
					lastWrite = currentWrite
				}
			case <-tc.stopChan:
				return
			}
		}
	}()
}

// 停止定期上报
func (tc *TrafficCounter) StopPeriodicReport() {
	close(tc.stopChan)
	tc.wg.Wait()
}

// 发送最终统计信息
func (tc *TrafficCounter) ReportFinal() {
	info := Info{
		Address:    tc.address,
		LocalPort:  tc.localPort,
		Bytes:      tc.BytesRead() + tc.BytesWritten(),
		Unix:       time.Now().Unix(),
		RepeatNums: 1,
		SessionID:  tc.sid,
		Domain:     tc.domain,
		ID:         generateUniqueID(),
		Timestamp:  time.Now().UnixNano(),
	}

	reportStats(info)

	log.Printf("[最终流量] SessionID=%s | Domain=%s | 上行=%d | 下行=%d | 总流量=%d | 耗时=%v",
		tc.sid, tc.domain, tc.BytesWritten(), tc.BytesRead(),
		tc.BytesRead() + tc.BytesWritten(), time.Since(tc.startTime))
}

func Transport(rw1, rw2 io.ReadWriter) error {
	errc := make(chan error, 1)
	go func() {
		errc <- CopyBuffer(rw1, rw2, bufferSize)
	}()

	go func() {
		errc <- CopyBuffer(rw2, rw1, bufferSize)
	}()

	if err := <-errc; err != nil && err != io.EOF {
		return err
	}
	return nil
}

func CopyBuffer(dst io.Writer, src io.Reader, bufSize int) error {
	buf := bufpool.Get(bufSize)
	defer bufpool.Put(buf)

	_, err := io.CopyBuffer(dst, src, buf)
	return err
}

// TransportWithStats 新版可靠传输实现
func TransportWithStats(rw1, rw2 io.ReadWriter, domain, sid string, localPort int) error {
	var (
		startTime = time.Now()
	)

	// 创建流量计数器
	counter1 := NewTrafficCounter(rw1, rw1, sid, domain, domain, localPort)
	counter2 := NewTrafficCounter(rw2, rw2, sid, domain, domain, localPort)

	// 启动定期上报，每500ms上报一次
	counter1.StartPeriodicReport(500 * time.Millisecond)
	counter2.StartPeriodicReport(500 * time.Millisecond)

	errc := make(chan error, 2)

	// 上行流量采集
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("上行协程异常: %v", r)
			}
			// 停止定期上报
			counter1.StopPeriodicReport()
		}()
		_, err := io.Copy(counter2, counter1)
		errc <- err
	}()

	// 下行流量采集
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("下行协程异常: %v", r)
			}
			// 停止定期上报
			counter2.StopPeriodicReport()
		}()
		_, err := io.Copy(counter1, counter2)
		errc <- err
	}()

	// 错误处理增强
	var errCount int
	for i := 0; i < 2; i++ {
		if err := <-errc; err != nil {
			if err != io.EOF {
				log.Printf("传输错误: %v", err)
				errCount++
			}
		}
	}

	// 发送最终统计信息
	counter1.ReportFinal()
	counter2.ReportFinal()

	if errCount > 0 {
		return io.ErrClosedPipe
	}
	return nil
}

// Transport1 统一传输接口
func Transport1(rw1, rw2 io.ReadWriter, address string, sid string) error {
	var (
		startTime = time.Now()
	)

	// 创建流量计数器
	counter1 := NewTrafficCounter(rw1, rw1, sid, address, "", 0)
	counter2 := NewTrafficCounter(rw2, rw2, sid, address, "", 0)

	// 启动定期上报，每500ms上报一次
	counter1.StartPeriodicReport(500 * time.Millisecond)
	counter2.StartPeriodicReport(500 * time.Millisecond)

	errc := make(chan error, 2)

	go func() {
		n, err := io.CopyBuffer(counter2, counter1, bufpool.Get(bufferSize))
		_ = n // 不再直接使用返回值，而是从计数器获取
		errc <- err
	}()

	go func() {
		n, err := io.CopyBuffer(counter1, counter2, bufpool.Get(bufferSize))
		_ = n // 不再直接使用返回值，而是从计数器获取
		errc <- err
	}()

	var errCount int
	for i := 0; i < 2; i++ {
		if err := <-errc; err != nil {
			if err != io.EOF {
				errCount++
			}
		}
	}

	// 停止定期上报
	counter1.StopPeriodicReport()
	counter2.StopPeriodicReport()

	// 发送最终统计信息
	counter1.ReportFinal()
	counter2.ReportFinal()

	if errCount > 0 {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func CopyBuffer1(dst io.Writer, src io.Reader, bufSize int, address string, sid string) error {
	buf := bufpool.Get(bufSize)
	defer bufpool.Put(buf)

	// 创建流量计数器
	counter := NewTrafficCounter(src, dst, sid, address, "", 0)

	// 启动定期上报，每500ms上报一次
	counter.StartPeriodicReport(500 * time.Millisecond)

	bytes, err := io.CopyBuffer(counter, counter, buf)
	log.Printf("[消耗流量：]--%s------%s------%s", address, bytes, sid)

	// 停止定期上报
	counter.StopPeriodicReport()

	// 发送最终统计信息
	counter.ReportFinal()

	return err
}

// CopyBufferWithStats 带统计的拷贝实现
func CopyBufferWithStats(dst io.Writer, src io.Reader, bufSize int, address, sid string) error {
	buf := bufpool.Get(bufSize)
	defer bufpool.Put(buf)

	startTime := time.Now()

	// 创建流量计数器
	counter := NewTrafficCounter(src, dst, sid, address, "", 0)

	// 启动定期上报，每500ms上报一次
	counter.StartPeriodicReport(500 * time.Millisecond)

	_, err := io.CopyBuffer(counter, counter, buf)

	// 停止定期上报
	counter.StopPeriodicReport()

	// 发送最终统计信息
	counter.ReportFinal()

	log.Printf("[流量统计] %s | 传输量=%d | 耗时=%v", sid, counter.BytesWritten(), time.Since(startTime))
	return err
}

type bufferReaderConn struct {
	net.Conn
	br *bufio.Reader
}

func NewBufferReaderConn(conn net.Conn, br *bufio.Reader) net.Conn {
	return &bufferReaderConn{
		Conn: conn,
		br:   br,
	}
}

func (c *bufferReaderConn) Read(b []byte) (int, error) {
	return c.br.Read(b)
}
