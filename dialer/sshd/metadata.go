package sshd

import (
	"os"
	"time"

	mdata "github.com/liukeqqs/core/metadata"
	mdutil "github.com/liukeqqs/core/metadata/util"
	"golang.org/x/crypto/ssh"
)

type metadata struct {
	handshakeTimeout  time.Duration
	signer            ssh.Signer
	keepalive         bool
	keepaliveInterval time.Duration
	keepaliveTimeout  time.Duration
	keepaliveRetries  int
}

func (d *sshdDialer) parseMetadata(md mdata.Metadata) (err error) {
	const (
		handshakeTimeout = "handshakeTimeout"
		privateKeyFile   = "privateKeyFile"
		passphrase       = "passphrase"
	)

	if key := mdutil.GetString(md, privateKeyFile); key != "" {
		data, err := os.ReadFile(key)
		if err != nil {
			return err
		}

		pp := mdutil.GetString(md, passphrase)
		if pp == "" {
			d.md.signer, err = ssh.ParsePrivateKey(data)
		} else {
			d.md.signer, err = ssh.ParsePrivateKeyWithPassphrase(data, []byte(pp))
		}
		if err != nil {
			return err
		}
	}

	d.md.handshakeTimeout = mdutil.GetDuration(md, handshakeTimeout)

	if d.md.keepalive = mdutil.GetBool(md, "keepalive"); d.md.keepalive {
		d.md.keepaliveInterval = mdutil.GetDuration(md, "ttl", "keepalive.interval")
		d.md.keepaliveTimeout = mdutil.GetDuration(md, "keepalive.timeout")
		d.md.keepaliveRetries = mdutil.GetInt(md, "keepalive.retries")
	}
	return
}
