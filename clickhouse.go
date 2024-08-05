package pkg

import (
  "context"
	"encoding/json"
	"fmt"
  
	"github.com/ClickHouse/clickhouse-go/v2"
  "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/mstoykov/envconfig"
	"go.k6.io/k6/lib/types"
	"gopkg.in/guregu/null.v4"
	"strings"
	"time"
)

func init() {
	modules.Register("k6/x/clickhouse", new(TCP))
}

type Clickhouse struct{}

func (cl *Clickhouse) Connect(conn_string string) (*clickhouse.Conn, error) {
  clickConn, err := clickhouse.Open(conn_string)
	if err != nil {
		return nil, err
	}
  
  ctx := context.Background()
  
	return clickConn, nil
}

func (cl *Clickhouse) Close() error {
	err := cl.clickConn.Close()
	if err != nil {
		return err
	}
	return nil
}

func (cl *Clickhouse) Insert(conn net.Conn, data []byte) error {
	_, err := conn.Write(data)
	if err != nil {
		return err
	}

	return nil
}

func (cl *Clickhouse) Batch(conn net.Conn, data []byte) error {
	_, err := conn.Write(data)
	if err != nil {
		return err
	}

	return nil
}

type TCP struct{}

func (tcp *TCP) Connect(addr string) (net.Conn, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func (tcp *TCP) Write(conn net.Conn, data []byte) error {
	_, err := conn.Write(data)
	if err != nil {
		return err
	}

	return nil
}

func (tcp *TCP) Read(conn net.Conn, size int) ([]byte, error) {
	buf := make([]byte, size)
	_, err := conn.Read(buf)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func (tcp *TCP) WriteLn(conn net.Conn, data []byte) error {
	return tcp.Write(conn, append(data, []byte("\n")...))
}

func (tcp *TCP) Close(conn net.Conn) error {
	err := conn.Close()
	if err != nil {
		return err
	}
	return nil
}
