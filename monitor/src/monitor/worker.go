package monitor

import (
	"context"
	"fmt"
	"github.com/jackc/pgconn"
	"log"
)

func (node *AutoNode)connect_db() (*pgconn.PgConn, error) {
	conn_info := fmt.Sprintf("user=hawu host=%s port=%d dbname=postgres", node.host, node.port)
	return pgconn.Connect(context.Background(), conn_info)
}

func (node *AutoNode)disconnect_db() {
	assert(node.conn != nil)
	err := node.conn.Close(context.Background())
	if err != nil {
		log.Println("Close database connection:", err)
	}
	node.conn = nil
}
func start_worker(node *AutoNode, resChan chan ResponseMessage)  {

	log.Println("Start worker loop for ", node, node.quit)
	var err error
	var cmdMessage CommandMessage
	var resMessage ResponseMessage

	resMessage.owner = node

	wait_time := COLLECTOR_IDLE_TIMEOUT
	err_cnt := 0

	workerFor:
	for !node.quit {
		timeout := true
		if err_cnt > 3 {
			resMessage.messageType = cmdMessage.messageType
			resMessage.returnCode = MRC_ERROR
			resChan <- resMessage
			err_cnt = 0
		}
		log.Println("Start to wait")
		node.T.Reset(wait_time)
	waitFor:
		for {
			select {
			case cmdMessage = <- node.cmdMessage:
				{
					log.Println("received message:", node, cmdMessage.messageType, cmdMessage.delay)
					timeout = false
					if !node.T.Stop() {
						<-node.T.C
					}
					if cmdMessage.delay == 0 {
						break waitFor
					}
					node.T.Reset(cmdMessage.delay)
				}
			case <- node.T.C:
				break waitFor

			}
		}
		if timeout {
			// ai is invalid, run default collect
			debug4("Time, the group doesn't send the command to this node: '%s'\n", node)
			cmdMessage.messageType = MSG_STATUS
		}
		if resMessage.messageType == MSG_EXIT {
			node.quit = true
			// FIXME: shall we do come cleanup?
			//  1. close the channel for command message
			//  2. close the database connection
			log.Printf("'%s' received EXIT message, the work will exit...\n", node)
			break workerFor
		}
		resMessage.messageType = cmdMessage.messageType
		if node.conn == nil {
			conn, err := node.connect_db()
			if err != nil {
				log.Println(err)
				wait_time = COLLECTOR_RETRY_TIMEOUT
				err_cnt++
				continue
			}
			node.conn = conn
		}
		err = node.processMessage(&cmdMessage, &resMessage)
		if err != nil {
			log.Println(err)
			wait_time = COLLECTOR_RETRY_TIMEOUT
			err_cnt++
			node.disconnect_db()
			continue
		}
		resChan <- resMessage
		err_cnt = 0
		wait_time = COLLECTOR_IDLE_TIMEOUT * 10
	}

}
func (node *AutoNode)processMessage(cmdMessage *CommandMessage, resMessage *ResponseMessage) error {
	var sql string
	switch cmdMessage.messageType {
	case MSG_STATUS: sql = "select role,syncrep,walconn from autofailover_execute('status', '?')"
	case MSG_UNSYNCREP: sql = "select role,syncrep,walconn from autofailover_execute('unsyncrep', '?')"
	case MSG_SYNCREP: sql = "select role,syncrep,walconn from autofailover_execute('syncrep', '?')"
	case MSG_PROMOTE: sql = "select role,syncrep,walconn from autofailover_execute('promote', '?')"
	case MSG_WRITE_QUERY:
		if node._has_temp_table {
			sql = "insert into autofailover_write_temp_table values(1);"
		} else {
			sql = "create temp table autofailover_write_temp_table as select 1 i;"
		}
		sql += "select role,syncrep,walconn from autofailover_execute('status', 'p');"
	default:
		log.Fatalln("unknown command from group: ", cmdMessage)
	}

	results, err:=node.conn.Exec(context.Background(), sql).ReadAll()
	if err != nil {
		log.Println("exec error = ", err, " sql=", sql)
		resMessage.returnCode = MRC_ERR_EXECUTE_SQL
		return err
	}
	//for idx,n:=0,len(results); idx<n; idx++ {
	//	log.Println(idx, len(results[idx].Rows))
	//}
	assert(len(results) >= 1)
	result := results[len(results)-1]
	if result.Err != nil {
		log.Println("multi-reader:", result.Err)
		resMessage.returnCode = MRC_ERR_EXECUTE_SQL
		return result.Err
	}
	//debug4("size: len(result.Rows), len(result.Rows[0]) = %d %d",
	//		len(result.Rows), len(result.Rows[0]))
	if len(result.Rows) != 1 || len(result.Rows[0]) != 3 {
		log.Println("unexpected data form: collect_status: ", result.Rows)
		resMessage.returnCode = MRC_ERR_SIZE
		return MRC_ERR_SIZE
	}
	//debug4("size of cols: %d %d %d",
	//	len(result.Rows[0][0]),
	//	len(result.Rows[0][1]),
	//	len(result.Rows[0][2]))
	resMessage.role = result.Rows[0][0][0]
	resMessage.syncrep = string(result.Rows[0][1])
	assert(string(result.Rows[0][2][0])=="t" || string(result.Rows[0][2][0])=="f")
	resMessage.walconn = result.Rows[0][2][0] == 't'
	// build result data
	resMessage.returnCode = MRC_OK

	return nil
}