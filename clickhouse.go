package k6clickhouse

import (
	"context"
	"fmt"
	"log"
	"go.k6.io/k6/js/modules"
	"database/sql"
	"github.com/ClickHouse/clickhouse-go/v2"
	// "net"
	"time"
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
    pool	     *sql.DB
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
	
	// dialCount := 0
	var (
		ctx       = context.Background()
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{fmt.Sprintf("%s:%d", host, port)},
			Auth: clickhouse.Auth{
				Database: database,
				Username: username,
				Password: password,
			},
			// DialContext: func(ctx context.Context, addr string) (net.Conn, error) {
			// 	dialCount++
			// 	var d net.Dialer
			// 	return d.DialContext(ctx, "tcp", addr)
			// },
			Protocol: clickhouse.HTTP,
			// Debug: true,
			// Debugf: func(format string, v ...any) {
			// 	fmt.Printf(format, v)
			// },
			Settings: clickhouse.Settings{
				"max_execution_time": 60,
			},
			DialTimeout: 5 * time.Second,
			Compression: &clickhouse.Compression{
				Method: clickhouse.CompressionLZ4,
			},
		})
	)
	if err != nil {
		log.Print("connect error")
		log.Print(err)
		//log.Fatal(err)
	}

	// v, err := conn.ServerVersion()
	// log.Print("version")
	// fmt.Println(v)
	// log.Print("version-err")
	// log.Print(err)
	// log.Print("host,port")
	// fmt.Printf("%s:%d", host, port)
	// log.Print("database")
	// log.Print(database)
	// log.Print("username")
	// log.Print(username)
	// log.Print("password")
	// log.Print(password)
	
	if err := conn.Exec(ctx, data); err != nil {
		
		log.Print("query error")
		// log.Print(data)
		log.Print(err)
		//log.Fatal(err)
	}
	log.Print("end connect1")
	return nil

	
}

func (c *Compare) Connect2( host string, port int, database string, username string, password string, data string ) error {
	
	conn, err := sql.Open("clickhouse", fmt.Sprintf("clickhouse://%s:%d/%s?username=%s&password=%s&dial_timeout=10s&connection_open_strategy=round_robin&debug=true&compress=lz4&secure=true", host, port, database, username, password))
	
	// // conn, err := sql.Open("clickhouse", fmt.Sprintf("http://%s:%d?username=%s&password=%s", env.Host, env.HttpPort, env.Username, env.Password))
	if err != nil {
		log.Print("connect error")
		log.Print(err)
		//log.Fatal(err)
	}
	
	if err = conn.Ping(); err != nil {
		log.Print("ping error")
		log.Print(err)
	}

	return nil
	
}

func (c *Compare) Connect3( host string, port int, database string, username string, password string, data string ) error {
	
	conn, err := sql.Open("clickhouse", fmt.Sprintf("clickhouse://%s:%d/%s?username=%s&password=%s&dial_timeout=10s&connection_open_strategy=round_robin&debug=true&compress=lz4&secure=true&&skip_verify=true", host, port, database, username, password))
	
	// // conn, err := sql.Open("clickhouse", fmt.Sprintf("http://%s:%d?username=%s&password=%s", env.Host, env.HttpPort, env.Username, env.Password))
	if err != nil {
		log.Print("connect error")
		log.Print(err)
		//log.Fatal(err)
	}
	
	if err = conn.Ping(); err != nil {
		log.Print("ping error")
		log.Print(err)
	}

	return nil
	
}

func (c *Compare) Connect4( host string, port int, database string, username string, password string, data string ) error {
	
	conn, err := sql.Open("clickhouse", fmt.Sprintf("clickhouse://%s:%d/%s?username=%s&password=%s&dial_timeout=10s&connection_open_strategy=round_robin&debug=true&compress=lz4&secure=false&&skip_verify=false", host, port, database, username, password))
	
	// // conn, err := sql.Open("clickhouse", fmt.Sprintf("http://%s:%d?username=%s&password=%s", env.Host, env.HttpPort, env.Username, env.Password))
	if err != nil {
		log.Print("connect error")
		log.Print(err)
		//log.Fatal(err)
	}
	
	if err = conn.Ping(); err != nil {
		log.Print("ping error")
		log.Print(err)
	}

	return nil
	
}
func (c *Compare) Connect5( connstr string ) error {
	
	conn, err := sql.Open("clickhouse", connstr)
	
	// // conn, err := sql.Open("clickhouse", fmt.Sprintf("http://%s:%d?username=%s&password=%s", env.Host, env.HttpPort, env.Username, env.Password))
	if err != nil {
		log.Print("connect error")
		log.Print(err)
		//log.Fatal(err)
	}
	
	if err = conn.Ping(); err != nil {
		log.Print("ping error")
		log.Print(err)
	}

	return nil
	
}

func (c *Compare) Connect6( connstr string, data string ) error {

	// var (
	// 	ctx       = context.Background()
	// )
	conn, err := sql.Open("clickhouse", connstr)
	
	// // conn, err := sql.Open("clickhouse", fmt.Sprintf("http://%s:%d?username=%s&password=%s", env.Host, env.HttpPort, env.Username, env.Password))
	if err != nil {
		log.Print("connect error")
		log.Print(err)
		//log.Fatal(err)
	}
	
	// if err = conn.Ping(); err != nil {
	// 	log.Print("ping error")
	// 	log.Print(err)
	// }
	res, err := conn.Exec(data)
	if  err != nil {
		
		log.Print("query error")
		log.Print(data)
		log.Print(err)
		//log.Fatal(err)
	}
	log.Print(res)
	return nil
	
}

func (c *Compare) Connect7( connstr string, data string ) error {

	// var (
	// 	ctx       = context.Background()
	// )
	conn, err := sql.Open("clickhouse", connstr)
	
	// // conn, err := sql.Open("clickhouse", fmt.Sprintf("http://%s:%d?username=%s&password=%s", env.Host, env.HttpPort, env.Username, env.Password))
	if err != nil {
		log.Print("connect error")
		// log.Print(err)
		//log.Fatal(err)
	}
	res, err := conn.Exec(data)
	if  err != nil {
		
		// log.Print("query error")
		// log.Print(data)
		log.Print(err)
		//log.Fatal(err)
	}
	if 1 != 1 {
		log.Print(res)
	}
	
	return nil
	
}

func (c *Compare) Connect8( connstr string ) error {
	
	pool, err := sql.Open("clickhouse", connstr)
	
	if err != nil {
		log.Print("connect error")
	}
	
	pool.SetConnMaxLifetime(0)
	pool.SetMaxIdleConns(5000)
	pool.SetMaxOpenConns(5000)
	
	c.pool = pool
	
	return nil
}

func (c *Compare) Insert8( data string ) error {
	res, err := c.pool.Exec(data)
	if  err != nil {
		log.Print(err)
	}
	if 1 != 1 {
		log.Print(res)
	}
	return nil
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
