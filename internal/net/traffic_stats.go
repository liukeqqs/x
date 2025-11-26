package net

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8"
)

const (
	// 本地缓存文件路径
	localCacheDir = "./traffic_cache"
	// 本地缓存文件最大大小 (100MB)
	maxCacheFileSize = 100 * 1024 * 1024
	// 上传批处理大小
	batchSize = 100
	// Redis连接池配置
	redisPoolSize = 20
	// Redis超时时间
	redisTimeout = 5 * time.Second
)

// FormatKey 格式化Redis键
func FormatKey(str string) string {
	parts := strings.Split(str, ":")
	if len(parts) == 2 {
		return parts[0]
	}
	return str
}

// TrafficStats 精准流量统计和上传模块
type TrafficStats struct {
	// Redis客户端
	redisClient *redis.Client
	// Redis字符串加载器
	redisLoader *redisStringLoader
	// 本地缓存文件
	cacheFile *os.File
	// 缓存文件写入器
	cacheWriter *bufio.Writer
	// 缓存文件大小
	cacheFileSize int64
	// 缓存文件锁
	cacheFileMutex sync.Mutex
	// 上传队列
	uploadQueue chan Info
	// 本地缓存队列
	localCacheQueue chan Info
	// 统计信息
	pendingUploadCount  int64
	pendingLocalCount   int64
	totalUploadedCount  int64
	totalLocalCacheCount int64
	// 上传状态
	uploadStatusMutex sync.RWMutex
	uploadStatus      bool
	// 本地缓存状态
	localCacheStatusMutex sync.RWMutex
	localCacheStatus      bool
}

// NewTrafficStats 创建新的流量统计和上传模块
func NewTrafficStats(redisAddr, redisPassword string, redisDB int) *TrafficStats {
	// 创建本地缓存目录
	if err := os.MkdirAll(localCacheDir, 0755); err != nil {
		log.Fatalf("创建本地缓存目录失败: %v", err)
	}

	// 初始化Redis客户端
	redisClient := redis.NewClient(&redis.Options{
		Addr:         redisAddr,
		Password:     redisPassword,
		DB:           redisDB,
		PoolSize:     redisPoolSize,
		MinIdleConns: 5,
		DialTimeout:  redisTimeout,
		ReadTimeout:  redisTimeout,
		WriteTimeout: redisTimeout,
	})

	// 创建Redis字符串加载器
	redisLoader := &redisStringLoader{
		client: redisClient,
		key:    "gost", // 使用默认键名
	}

	// 测试Redis连接
	ctx, cancel := context.WithTimeout(context.Background(), redisTimeout)
	defer cancel()
	if err := redisClient.Ping(ctx).Err(); err != nil {
		log.Printf("警告: Redis连接失败，将使用本地缓存模式: %v", err)
	}

	// 创建流量统计和上传模块
	ts := &TrafficStats{
		redisClient:     redisClient,
		redisLoader:     redisLoader,
		uploadQueue:     make(chan Info, 10000),
		localCacheQueue: make(chan Info, 100000),
	}

	// 初始化本地缓存文件
	ts.initLocalCacheFile()

	// 启动上传协程
	go ts.uploadWorker()
	// 启动本地缓存上传协程
	go ts.localCacheUploadWorker()
	// 启动监控协程
	go ts.monitor()

	return ts
}

// initLocalCacheFile 初始化本地缓存文件
func (ts *TrafficStats) initLocalCacheFile() {
	// 生成本地缓存文件名
	filename := filepath.Join(localCacheDir, fmt.Sprintf("traffic_cache_%d.dat", time.Now().Unix()))
	
	// 创建本地缓存文件
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Printf("创建本地缓存文件失败: %v", err)
		return
	}
	
	// 初始化缓存文件写入器
	ts.cacheFile = file
	ts.cacheWriter = bufio.NewWriter(file)
	
	// 获取文件大小
	if stat, err := file.Stat(); err == nil {
		ts.cacheFileSize = stat.Size()
	}
	
	log.Printf("本地缓存文件初始化成功: %s", filename)
}

// ReportStats 上报流量统计信息
func (ts *TrafficStats) ReportStats(info Info) {
	// 只有在有流量传输时才上报
	if info.Bytes <= 0 {
		return
	}
	
	// 尝试直接上传
	select {
	case ts.uploadQueue <- info:
		atomic.AddInt64(&ts.pendingUploadCount, 1)
	default:
		// 如果上传队列已满，写入本地缓存
		select {
		case ts.localCacheQueue <- info:
			atomic.AddInt64(&ts.pendingLocalCount, 1)
		default:
			// 如果本地缓存队列也满了，丢弃数据并记录日志
			// 减少日志输出频率，避免日志刷屏
			// log.Printf("警告: 流量统计数据被丢弃，上传队列和本地缓存队列均已满")
		}
	}
}

// uploadWorker 上传协程
func (ts *TrafficStats) uploadWorker() {
	batch := make([]Info, 0, batchSize)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case info := <-ts.uploadQueue:
			atomic.AddInt64(&ts.pendingUploadCount, -1)
			batch = append(batch, info)
			
			// 如果批次已满，立即上传
			if len(batch) >= batchSize {
				ts.uploadBatch(batch)
				batch = make([]Info, 0, batchSize)
			}
		case <-ticker.C:
			// 定时上传批次
			if len(batch) > 0 {
				ts.uploadBatch(batch)
				batch = make([]Info, 0, batchSize)
			}
		}
	}
}

// localCacheUploadWorker 本地缓存上传协程
func (ts *TrafficStats) localCacheUploadWorker() {
	for {
		select {
		case info := <-ts.localCacheQueue:
			atomic.AddInt64(&ts.pendingLocalCount, -1)
			ts.writeToLocalCache(info)
		default:
			// 如果本地缓存队列为空，尝试从文件读取数据并上传
			ts.uploadFromLocalCache()
			time.Sleep(5 * time.Second) // 降低轮询频率，减少CPU使用
		}
	}
}

// uploadBatch 上传批次数据
func (ts *TrafficStats) uploadBatch(batch []Info) {
	// 设置上传状态为忙碌
	ts.setUploadStatus(true)
	defer ts.setUploadStatus(false)

	// 检查Redis客户端是否可用
	ctx, cancel := context.WithTimeout(context.Background(), redisTimeout)
	defer cancel()
	if err := ts.redisClient.Ping(ctx).Err(); err != nil {
		// 减少日志输出频率，避免日志刷屏
		// log.Printf("Redis连接不可用，写入本地缓存: %v", err)
		// Redis不可用，写入本地缓存
		for _, info := range batch {
			ts.writeToLocalCache(info)
		}
		return
	}

	// 保持与原始实现一致的数据存储方式
	successCount := 0
	for _, info := range batch {
		key := FormatKey(info.Address)
		// 使用GetValSet方法保持与原始实现一致的行为
		if err := ts.redisLoader.GetValSet(ctx, key, info); err != nil {
			// 减少日志输出频率，避免日志刷屏
			// log.Printf("上传流量统计数据失败: %v", err)
			// 上传失败，写入本地缓存
			ts.writeToLocalCache(info)
		} else {
			successCount++
		}
	}
	
	atomic.AddInt64(&ts.totalUploadedCount, int64(successCount))
	// 减少日志输出频率，避免日志刷屏
	// log.Printf("批量上传流量统计数据成功: %d条", len(batch))
}

// writeToLocalCache 写入本地缓存
func (ts *TrafficStats) writeToLocalCache(info Info) {
	ts.cacheFileMutex.Lock()
	defer ts.cacheFileMutex.Unlock()

	// 检查文件大小，如果超过限制则创建新文件
	if ts.cacheFileSize >= maxCacheFileSize {
		ts.rotateCacheFile()
	}

	// 只有序列化有流量的数据
	if info.Bytes <= 0 {
		return
	}

	// 序列化数据
	data, err := json.Marshal(info)
	if err != nil {
		// 减少日志输出频率，避免日志刷屏
		// log.Printf("序列化流量统计数据失败: %v", err)
		return
	}

	// 检查文件是否仍然有效
	if ts.cacheFile == nil || ts.cacheWriter == nil {
		// 减少日志输出频率，避免日志刷屏
		// log.Printf("本地缓存文件未初始化")
		return
	}

	// 写入文件
	if _, err := ts.cacheWriter.Write(append(data, '\n')); err != nil {
		// 减少日志输出频率，避免日志刷屏
		// log.Printf("写入本地缓存文件失败: %v", err)
		// 尝试重新初始化文件
		ts.rotateCacheFile()
		return
	}

	// 更新文件大小
	ts.cacheFileSize += int64(len(data) + 1)
	atomic.AddInt64(&ts.totalLocalCacheCount, 1)
	
	// 刷新缓冲区
	if err := ts.cacheWriter.Flush(); err != nil {
		// 减少日志输出频率，避免日志刷屏
		// log.Printf("刷新本地缓存文件缓冲区失败: %v", err)
	}
}

// rotateCacheFile 轮转缓存文件
func (ts *TrafficStats) rotateCacheFile() {
	ts.cacheFileMutex.Lock()
	defer ts.cacheFileMutex.Unlock()

	// 关闭当前文件
	if ts.cacheWriter != nil {
		ts.cacheWriter.Flush()
	}
	if ts.cacheFile != nil {
		ts.cacheFile.Close()
	}

	// 创建新文件
	ts.initLocalCacheFile()
}

// uploadFromLocalCache 从本地缓存上传数据
func (ts *TrafficStats) uploadFromLocalCache() {
	// 设置本地缓存状态为忙碌
	ts.setLocalCacheStatus(true)
	defer ts.setLocalCacheStatus(false)

	// 查找最早的缓存文件
	cacheFiles, err := filepath.Glob(filepath.Join(localCacheDir, "traffic_cache_*.dat"))
	if err != nil || len(cacheFiles) == 0 {
		return
	}

	// 选择最早的文件
	cacheFile := cacheFiles[0]
	for _, file := range cacheFiles {
		if file < cacheFile {
			cacheFile = file
		}
	}

	// 打开缓存文件
	file, err := os.Open(cacheFile)
	if err != nil {
		// 减少日志输出频率，避免日志刷屏
		// log.Printf("打开本地缓存文件失败: %v", err)
		return
	}
	defer file.Close()

	// 读取文件内容
	scanner := bufio.NewScanner(file)
	batch := make([]Info, 0, batchSize)
	
	processedCount := 0
	for scanner.Scan() {
		var info Info
		if err := json.Unmarshal(scanner.Bytes(), &info); err != nil {
			// 减少日志输出频率，避免日志刷屏
			// log.Printf("反序列化本地缓存数据失败: %v", err)
			continue
		}
		
		// 只处理有流量的数据
		if info.Bytes > 0 {
			batch = append(batch, info)
			processedCount++
		}
		
		// 如果批次已满，上传数据
		if len(batch) >= batchSize {
			// 尝试上传批次
			if ts.tryUploadBatch(batch) {
				// 上传成功，清空批次
				batch = make([]Info, 0, batchSize)
			} else {
				// 上传失败，停止处理
				break
			}
		}
	}
	
	// 上传剩余数据
	if len(batch) > 0 {
		ts.tryUploadBatch(batch)
		processedCount += len(batch)
	}
	
	// 如果文件已完全处理，删除文件
	// 检查是否已处理完所有行
	if processedCount > 0 {
		fileInfo, err := file.Stat()
		if err == nil {
			// 简单检查文件是否已处理完（通过文件大小估算）
			// 这里使用一个简单的启发式方法：如果处理的行数乘以平均行大小接近文件大小，则认为已处理完
			avgLineSize := 200 // 假设平均每行200字节
			if processedCount*avgLineSize >= int(fileInfo.Size()) {
				file.Close()
				if err := os.Remove(cacheFile); err != nil {
					// 减少日志输出频率，避免日志刷屏
					// log.Printf("删除本地缓存文件失败: %v", err)
				} else {
					// 减少日志输出频率，避免日志刷屏
					// log.Printf("本地缓存文件已处理并删除: %s", cacheFile)
				}
			}
		}
	}
}

// tryUploadBatch 尝试上传批次数据
func (ts *TrafficStats) tryUploadBatch(batch []Info) bool {
	// 检查Redis客户端是否可用
	ctx, cancel := context.WithTimeout(context.Background(), redisTimeout)
	defer cancel()
	if err := ts.redisClient.Ping(ctx).Err(); err != nil {
		// 减少日志输出频率，避免日志刷屏
		// log.Printf("Redis连接不可用，无法上传本地缓存数据: %v", err)
		return false
	}

	// 保持与原始实现一致的数据存储方式
	successCount := 0
	for _, info := range batch {
		key := FormatKey(info.Address)
		// 使用GetValSet方法保持与原始实现一致的行为
		if err := ts.redisLoader.GetValSet(ctx, key, info); err != nil {
			// 减少日志输出频率，避免日志刷屏
			// log.Printf("从本地缓存上传流量统计数据失败: %v", err)
			return false
		} else {
			successCount++
		}
	}
	
	atomic.AddInt64(&ts.totalUploadedCount, int64(successCount))
	// 减少日志输出频率，避免日志刷屏
	// log.Printf("从本地缓存批量上传流量统计数据成功: %d条", successCount)
	return true
}

// monitor 监控协程
func (ts *TrafficStats) monitor() {
	// 增加监控日志的时间间隔，减少日志输出频率
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		// 只有在有数据需要监控时才输出日志
		pendingUpload := atomic.LoadInt64(&ts.pendingUploadCount)
		pendingLocal := atomic.LoadInt64(&ts.pendingLocalCount)
		totalUploaded := atomic.LoadInt64(&ts.totalUploadedCount)
		totalLocalCache := atomic.LoadInt64(&ts.totalLocalCacheCount)
		
		// 只有在有待处理数据时才输出监控日志
		if pendingUpload > 0 || pendingLocal > 0 || totalUploaded > 0 || totalLocalCache > 0 {
			log.Printf("[流量统计监控] 待上传:%d 本地缓存:%d 已上传:%d 本地缓存总数:%d 上传状态:%v 本地缓存状态:%v",
				pendingUpload,
				pendingLocal,
				totalUploaded,
				totalLocalCache,
				ts.getUploadStatus(),
				ts.getLocalCacheStatus(),
			)
		}
	}
}

// setUploadStatus 设置上传状态
func (ts *TrafficStats) setUploadStatus(status bool) {
	ts.uploadStatusMutex.Lock()
	defer ts.uploadStatusMutex.Unlock()
	ts.uploadStatus = status
}

// getUploadStatus 获取上传状态
func (ts *TrafficStats) getUploadStatus() bool {
	ts.uploadStatusMutex.RLock()
	defer ts.uploadStatusMutex.RUnlock()
	return ts.uploadStatus
}

// setLocalCacheStatus 设置本地缓存状态
func (ts *TrafficStats) setLocalCacheStatus(status bool) {
	ts.localCacheStatusMutex.Lock()
	defer ts.localCacheStatusMutex.Unlock()
	ts.localCacheStatus = status
}

// getLocalCacheStatus 获取本地缓存状态
func (ts *TrafficStats) getLocalCacheStatus() bool {
	ts.localCacheStatusMutex.RLock()
	defer ts.localCacheStatusMutex.RUnlock()
	return ts.localCacheStatus
}

// Close 关闭流量统计和上传模块
func (ts *TrafficStats) Close() error {
	ts.cacheFileMutex.Lock()
	defer ts.cacheFileMutex.Unlock()

	// 关闭Redis客户端
	if ts.redisClient != nil {
		if err := ts.redisClient.Close(); err != nil {
			log.Printf("关闭Redis客户端失败: %v", err)
		}
	}

	// 关闭缓存文件
	if ts.cacheWriter != nil {
		if err := ts.cacheWriter.Flush(); err != nil {
			log.Printf("刷新本地缓存文件缓冲区失败: %v", err)
		}
		ts.cacheWriter = nil
	}
	if ts.cacheFile != nil {
		if err := ts.cacheFile.Close(); err != nil {
			log.Printf("关闭本地缓存文件失败: %v", err)
		}
		ts.cacheFile = nil
	}

	return nil
}

// redisStringLoader 用于与原始实现保持一致
type redisStringLoader struct {
	client *redis.Client
	key    string
}

func (p *redisStringLoader) GetValSet(ctx context.Context, key string, object interface{}) (err error) {
	var (
		data      string
		exist     = true
		_info     = &Info{} // 使用本地的Info类型
		paramByte []byte
	)
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()
	data, err = p.client.Get(ctx, key).Result()
	if err == redis.Nil {
		exist = false
		err = nil
	}

	if exist {
		var info = object.(Info) // 使用本地的Info类型

		err = json.Unmarshal([]byte(data), _info)
		if err != nil {
			return
		}
		info.Bytes += _info.Bytes
		info.RepeatNums = _info.RepeatNums + 1
		paramByte, err = json.Marshal(info)
		if err != nil {
			return
		}
		err = p.client.Set(ctx, key, string(paramByte), time.Hour*24*30).Err()
		if err != nil {
			return err
		}

		return
	}

	paramByte, err = json.Marshal(object.(Info)) // 使用本地的Info类型
	if err != nil {
		return
	}
	err = p.client.Set(ctx, key, string(paramByte), time.Hour*24*30).Err()
	if err != nil {
		return err
	}
	return nil

}




