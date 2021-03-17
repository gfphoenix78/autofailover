package main

import (
	"context"
	"errors"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgproto3"
	"log"
	"time"
)

func main()  {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	//g := monitor.MockGroupNormal()
	//g.Loop()

	standalone_main("")
}

type wTimeout struct {
	deadline time.Time
	ch chan struct{}
}

func (w wTimeout)Deadline()(deadline time.Time, ok bool) {
	return w.deadline, true
}
func (w wTimeout)Done() <-chan struct{} {
	return w.ch
}
var _wq_timeout = errors.New("WQ-Timeout")
func (wTimeout)Err() error {
	return _wq_timeout
}
func (wTimeout)Value(key interface{}) interface{} {
	return nil
}
func standalone_main(cmd string)  {
	conn_info := "dbname=postgres user=hawu"
	pgConn, err := pgconn.Connect(context.Background(), conn_info)
	if err != nil {
		log.Fatalln("pgconn failed to connect:", err)
	}
	log.Println(pgConn)
	defer pgConn.Close(context.Background())

	var resultReader *pgconn.ResultReader
	var result *pgconn.Result
	var desc []pgproto3.FieldDescription

	//resultReader := pgConn.ExecParams(context.Background(),
	//	"select * from autofailover_execute('$1', '?')",
	//	[][]byte{[]byte(cmd)}, nil,
	//	nil, nil)

	//resultReader := pgConn.ExecParams(context.Background(),
	//	"create temp table af_table(i int);select role,syncrep,walconn from autofailover_execute('status', '?')",
	//	nil, nil,
	//	nil, nil)
	//log.Println(resultReader.FieldDescriptions())
	//result := resultReader.Read()
	//desc :=result.FieldDescriptions
	//log.Printf("count of rows = %d, err = %s\n", len(result.Rows), result.Err)
	//
	//
	//for idx, row := range result.Rows[0] {
	//	log.Printf("%s => '%s' %d\n", string(desc[idx].Name), string(row), len(row))
	//}

	//wq := wTimeout{
	//	deadline: time.Now().Add(10*time.Second),
	//	ch: make(chan struct{}),
	//}
	//
	//go func() {
	//	time.Sleep(time.Second*30)
	//	log.Println("timeout ....")
	//	//wq.ch <- struct{}{}
	//	close(wq.ch)
	//}()
	ctx, cfn := context.WithTimeout(context.Background(), time.Second*10)
	defer cfn()
	mulReader := pgConn.Exec(ctx,
		"create temp table af_table(i int);")
	defer mulReader.Close()
	{
		rs, err := mulReader.ReadAll()
		log.Println("len results = ", len(rs), err)

	}
	for mulReader.NextResult() {
		resultReader = mulReader.ResultReader()
		result = resultReader.Read();
		log.Println("ERROR = ", result.Err)
		desc = result.FieldDescriptions

		for idx, d := range desc {
			log.Printf("[%d] - %s\n", idx, d.Name)
		}

		if len(result.Rows) > 0 {
			for idx, row := range result.Rows[0] {
				log.Printf("%s => '%s' %d\n", string(desc[idx].Name), string(row), len(row))
			}
		}
	}
}