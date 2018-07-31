package flow

import (
	"github.com/golang/snappy"
	"net"
)

// Snappy Compressor To Compress KCP stream along the network
// This can also be used in TCP. This implements io.ReadWriteCloser
type SnappyStreamCompressor struct {
	conn net.Conn
	w    *snappy.Writer
	r    *snappy.Reader
}

func (c *SnappyStreamCompressor) Read(p []byte) (n int, err error) {
	return c.r.Read(p)
}

func (c *SnappyStreamCompressor) Write(b []byte) (n int, err error) {
	n, err = c.w.Write(b)
	err = c.w.Flush()
	return n, err
}

func (c *SnappyStreamCompressor) Close() error {
	return c.conn.Close()
}

func newSnappyStreamCompressor(conn net.Conn) *SnappyStreamCompressor {
	return &SnappyStreamCompressor{
		conn: conn,
		w:    snappy.NewBufferedWriter(conn),
		r:    snappy.NewReader(conn),
	}
}
