package clickhouse

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
	modules.Register("k6/x/clickhouse", new(Clickhouse))
}

type Clickhouse struct{}

// type Client struct {
// 	client *Clickhouse.Client
// }

func (cl *Clickhouse) Connect(connURI string) (*clickhouse.Conn, error) {
	clickConn, err := clickhouse.Open(connURI)
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

