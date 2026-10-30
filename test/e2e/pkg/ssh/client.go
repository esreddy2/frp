package ssh

import (
	"net"
	"os"

	libio "github.com/fatedier/golib/io"
	"golang.org/x/crypto/ssh"
)

type TunnelClient struct {
	localAddr string
	sshServer string
	commands  string

	sshConn *ssh.Client
	ln      net.Listener
}

func NewTunnelClient(localAddr string, sshServer string, commands string) *TunnelClient {
	return &TunnelClient{
		localAddr: localAddr,
		sshServer: sshServer,
		commands:  commands,
	}
}

func (c *TunnelClient) Start() error {
	hostKey, err := loadServerHostKey()
	if err != nil {
		return err
	}

	config := &ssh.ClientConfig{
		User:            "v0",
		HostKeyCallback: ssh.FixedHostKey(hostKey),
	}

	conn, err := ssh.Dial("tcp", c.sshServer, config)
	if err != nil {
		return err
	}
	c.sshConn = conn

	l, err := conn.Listen("tcp", "0.0.0.0:80")
	if err != nil {
		return err
	}
	c.ln = l
	ch, req, err := conn.OpenChannel("session", []byte(""))
	if err != nil {
		return err
	}
	defer ch.Close()
	go ssh.DiscardRequests(req)

	type command struct {
		Cmd string
	}
	_, err = ch.SendRequest("exec", false, ssh.Marshal(command{Cmd: c.commands}))
	if err != nil {
		return err
	}

	go c.serveListener()
	return nil
}

func loadServerHostKey() (ssh.PublicKey, error) {
	keyBytes, err := os.ReadFile(".autogen_ssh_key")
	if err != nil {
		return nil, err
	}
	signer, err := ssh.ParsePrivateKey(keyBytes)
	if err != nil {
		return nil, err
	}
	return signer.PublicKey(), nil
}

func (c *TunnelClient) Close() {
	if c.sshConn != nil {
		_ = c.sshConn.Close()
	}
	if c.ln != nil {
		_ = c.ln.Close()
	}
}

func (c *TunnelClient) serveListener() {
	for {
		conn, err := c.ln.Accept()
		if err != nil {
			return
		}
		go c.handleConn(conn)
	}
}

func (c *TunnelClient) handleConn(conn net.Conn) {
	defer conn.Close()
	local, err := net.Dial("tcp", c.localAddr)
	if err != nil {
		return
	}
	_, _, _ = libio.Join(local, conn)
}
