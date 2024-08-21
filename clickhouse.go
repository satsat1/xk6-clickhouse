package k6clickhouse

import (
	"context"
	"fmt"
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
