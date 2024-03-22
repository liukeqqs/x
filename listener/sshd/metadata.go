package ssh

import (
	"os"

	mdata "github.com/liukeqqs/core/metadata"
	mdutil "github.com/liukeqqs/core/metadata/util"
	ssh_util "github.com/liukeqqs/x/internal/util/ssh"
	"golang.org/x/crypto/ssh"
)

const (
	defaultBacklog = 128
)

type metadata struct {
	signer         ssh.Signer
	authorizedKeys map[string]bool
	backlog        int
	mptcp          bool
}

func (l *sshdListener) parseMetadata(md mdata.Metadata) (err error) {
	const (
		authorizedKeys = "authorizedKeys"
		privateKeyFile = "privateKeyFile"
		passphrase     = "passphrase"
		backlog        = "backlog"
	)

	if key := mdutil.GetString(md, privateKeyFile); key != "" {
		data, err := os.ReadFile(key)
		if err != nil {
			return err
		}

		pp := mdutil.GetString(md, passphrase)
		if pp == "" {
			l.md.signer, err = ssh.ParsePrivateKey(data)
		} else {
			l.md.signer, err = ssh.ParsePrivateKeyWithPassphrase(data, []byte(pp))
		}
		if err != nil {
			return err
		}
	}
	if l.md.signer == nil {
		signer, err := ssh.NewSignerFromKey(l.options.TLSConfig.Certificates[0].PrivateKey)
		if err != nil {
			return err
		}
		l.md.signer = signer
	}

	if name := mdutil.GetString(md, authorizedKeys); name != "" {
		m, err := ssh_util.ParseAuthorizedKeysFile(name)
		if err != nil {
			return err
		}
		l.md.authorizedKeys = m
	}

	l.md.backlog = mdutil.GetInt(md, backlog)
	if l.md.backlog <= 0 {
		l.md.backlog = defaultBacklog
	}

	l.md.mptcp = mdutil.GetBool(md, "mptcp")
	return
}
