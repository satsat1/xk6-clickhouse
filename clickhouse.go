package k6clickhouse

import (
	"context"
	"fmt"
	"log"
	"go.k6.io/k6/js/modules"
	"github.com/ClickHouse/clickhouse-go/v2"
)

// init is called by the Go runtime at application startup.
func init() {
    modules.Register("k6/x/k6clickhouse", new(Compare))
}

// Compare is the type for our custom API.
type Compare struct{
    ComparisonResult string // textual description of the most recent comparison
    clickConn        clickhouse.Conn
	ctx              context.Context
}

// IsGreater returns true if a is greater than b, or false otherwise, setting textual result message.
func (c *Compare) IsGreater(a, b int) bool {
    if a > b {
        c.ComparisonResult = fmt.Sprintf("%d is greater than %d", a, b)
        return true
    } else {
        c.ComparisonResult = fmt.Sprintf("%d is NOT greater than %d", a, b)
        return false
    }
}

func (c *Compare) Connect( host string, port int, database string, username string, password string ) error {
	// clickConn, err := clickhouse.Open(connURI)
	// if err != nil {
	// 	return nil, err
	// }
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
	
	c.clickConn = clickConn
	c.ctx = context.Background()
	
	return nil
	// return &Clickhouse{
	// 	clickConn:	clickConn,
	// 	ctx:		context.Background()
	// }
}

func (c *Compare) Connect1( host string, port int, database string, username string, password string, data string ) error {
	// clickConn, err := clickhouse.Open(connURI)
	// if err != nil {
	// 	return nil, err
	// }
	var (
		ctx       = context.Background()
		clickConn, err := clickhouse.Open(&clickhouse.Options{
			Addr: []string{fmt.Sprintf("%s:%d", host, port)},
			Auth: clickhouse.Auth{
				Database: database,
				Username: username,
				Password: password,
			},
		})
	)
	if err != nil {
		log.Fatal(err)
	}

	if err := clickConn.Exec(ctx, data); err != nil {
		log.Fatal(err)
	}
	
	// //clickConn.Exec(context.Background(), data)
	// err1 := clickConn.Exec(context.Background(), data)
	// if err1 != nil {
	// 	return err1
	// }

	return nil

	// var (
	// 	ctx       = context.Background()
	// 	conn, err = clickhouse.Open(&clickhouse.Options{
	// 		Addr: []string{"127.0.0.1:9000"},
	// 		Auth: clickhouse.Auth{
	// 			Database: "default",
	// 			Username: "default",
	// 			Password: "",
	// 		},
	// 		//Debug:           true,
	// 		DialTimeout:     time.Second,
	// 		MaxOpenConns:    10,
	// 		MaxIdleConns:    5,
	// 		ConnMaxLifetime: time.Hour,
	// 	})
	// )
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// if err := conn.Exec(ctx, "DROP TABLE IF EXISTS benchmark_async"); err != nil {
	// 	log.Fatal(err)
	// }
	// if err := conn.Exec(ctx, ddl); err != nil {
	// 	log.Fatal(err)
	// }
	// start := time.Now()
	// if err := benchmark(conn); err != nil {
	// 	log.Fatal(err)
	// }
	// fmt.Println(time.Since(start))
	
}

func (c *Compare) Close() error {
	err := c.clickConn.Close()
	if err != nil {
		return err
	}
	return nil
}

func (c *Compare) Insert(data string) error {
	err := c.clickConn.Exec(c.ctx, data)
	if err != nil {
		return err
	}

	return nil
}
