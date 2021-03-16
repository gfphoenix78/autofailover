package main

import (
	"context"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgproto3"
	"log"
	"os"
)

func main()  {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	//monitor.Main2()
	cmd := "status"
	if len(os.Args) > 1 {
		cmd = os.Args[1]
	}
	standalone_main(cmd)
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


	mulReader := pgConn.Exec(context.Background(),
		"create temp table af_table(i int);select role,syncrep,walconn from autofailover_execute('status', '?')")
	defer mulReader.Close()
	for mulReader.NextResult() {
		resultReader = mulReader.ResultReader()
		result = resultReader.Read();
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