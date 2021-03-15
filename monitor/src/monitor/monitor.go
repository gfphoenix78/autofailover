package monitor

import (
	"context"
	"net"
	"time"
)
import "github.com/jackc/pgconn"
import "fmt"
import "log"

type auto_node struct {
	name string
	host string
	datadir string
	port int
	state *auto_node_state
}
type node_status struct {
	role byte
	walconn bool
	health bool // if node is not health, we always probes
	syncrep string
	sync_state string
	lsn string

	time_updated time.Time
	time_disconnect time.Time

	// channel to receive request
	cmd_chan chan ActionItem
	// timer
	T *time.Timer
	quit bool
}
type auto_node_state struct {
	node_status
	conn *pgconn.PgConn
}
type auto_group struct {
	// target node,
	// INIT0 : nil
	// SINGLE: node
	// NORMAL: primary
	// PROMOTINIG: the node to be promoted
	target *auto_node
	nodes []*auto_node

	ID int
	sysid string
	state byte
	wal_sync byte // '?' 't' 'f'

	promoting_time time.Time

	quit chan bool
	resMessage chan ServerResponseMessage
}

// register group
// add node
// del node
// unregister group

type auto_monitor struct {
	ctlConn net.Conn
	groups []*auto_group
}
func runSimpleQuery(pgConn *pgconn.PgConn, sql string) *pgconn.Result {
	return pgConn.ExecParams(context.Background(), sql, nil, nil, nil, nil).Read()
}
func Main0() {
	conninfo := "host=localhost port=5432 dbname=postgres user=hawu "
	pgConn, err := pgconn.Connect(context.Background(), conninfo)
	if err != nil {
		log.Fatalln(err)
	}
	defer pgConn.Close(context.Background())
	fmt.Println("connect OK,", pgConn)

	var sql string
	var result *pgconn.Result
	sql = "select generate_series(1,3)"
	sql = "create extension if not exists autofailover"
	sql = "select * from autofailover_execute('status', '?')"
	//sql = "select execute_action('promote')"

	//result := pgConn.ExecParams(context.Background(), sql, nil, nil, nil, nil).Read()
	result = runSimpleQuery(pgConn, sql)
	if result.Err != nil {
		log.Fatalln(result.Err)
	}
	log.Println(result.Rows)
	fmt.Println("Exec result = ", result)
	fmt.Printf("====> %T %v %d\n", result.Rows, result.Rows, len(result.Rows))
	fmt.Println(len(result.Rows), len(result.Rows[0]), len(result.Rows[0][0]))

	for _, row := range result.Rows {
		fmt.Println(string(row[0]))
fmt.Printf("###=> %T %v, %d\n", row, row, len(row))
		for _, field := range row {
			fmt.Print(string(field))
			fmt.Println("  ???")
		}
		fmt.Println("##########")
	}

	fmt.Println(result.CommandTag)
//	var ctl auto_monitor
//	ctl.RunControlServer()
}

func Main1()  {
	g := buildGroupSingle()
	g.run_loop()
}

func Main2()  {
	g := buildGroupNormal()
	g.run_loop()
}