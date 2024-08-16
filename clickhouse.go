package clickhouse

import (
	"context"
	// "encoding/json"
	"fmt"
	"go.k6.io/k6/js/modules"

	"github.com/ClickHouse/clickhouse-go/v2"
	// "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	// "github.com/mstoykov/envconfig"
	// "go.k6.io/k6/lib/types"
	// "gopkg.in/guregu/null.v4"
	// "strings"
	// "time"
)

func init() {
	modules.Register("k6/x/clickhouse", new(Clickhouse))
}

type Clickhouse struct{
	clickConn		clickhouse.Conn
	ctx			context.Context
}

// type Client struct {
// 	client *Clickhouse.Client
// }

func (cl *Clickhouse) Connect( host string, port int, database string, username string, password string ) error {
	// clickConn, err := clickhouse.Open(connURI)
	// if err != nil {
	// 	return nil, err
	// }
conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{"127.0.0.1:9000"},
			Auth: clickhouse.Auth{
				Database: "default",
				Username: "default",
				Password: "",
			},
			//Debug:           true,
			DialTimeout:     time.Second,
			MaxOpenConns:    10,
			MaxIdleConns:    5,
			ConnMaxLifetime: time.Hour,
		})
	
	clickConn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", host, port)},
		Auth: clickhouse.Auth{
			Database: database,
			Username: username,
			Password: password,
		},
	})
	if err != nil {
		return err
	}
	
	cl.clickConn = clickConn
	cl.ctx = context.Background()
	
	return nil
	// return &Clickhouse{
	// 	clickConn:	clickConn,
	// 	ctx:		context.Background()
	// }
}

func (cl *Clickhouse) Close() error {
	err := cl.clickConn.Close()
	if err != nil {
		return err
	}
	return nil
}

func (cl *Clickhouse) Insert(data string) error {
	_, err := cl.clickConn.Exec(cl.ctx, data)
	if err != nil {
		return err
	}

	return nil
}

// func (cl *Clickhouse) Batch(data []byte) error {
// 	_, err := cl.clickConn.Write(data)
// 	if err != nil {
// 		return err
// 	}

// 	return nil
// }
