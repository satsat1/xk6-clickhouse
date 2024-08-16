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

type Clickhouse struct{
	clickConn clickhouse.Conn
}

// type Client struct {
// 	client *Clickhouse.Client
// }

func (cl *Clickhouse) Connect( host string, port int, database string, username string, password string) (*clickhouse.Conn, error) {
	// clickConn, err := clickhouse.Open(connURI)
	// if err != nil {
	// 	return nil, err
	// }

	cl.clickConn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", host, port)},
		Auth: clickhouse.Auth{
			Database: database,
			Username: username,
			Password: password,
		},
	})
	if err != nil {
		return nil, err
	}

	ctx := context.Background()

	return conn, nil
}

func (cl *Clickhouse) Close() error {
	err := cl.clickConn.Close()
	if err != nil {
		return err
	}
	return nil
}

func (cl *Clickhouse) Insert(data []byte) error {
	_, err := cl.clickConn.Write(data)
	if err != nil {
		return err
	}

	return nil
}

func (cl *Clickhouse) Batch(data []byte) error {
	_, err := cl.clickConn.Write(data)
	if err != nil {
		return err
	}

	return nil
}
