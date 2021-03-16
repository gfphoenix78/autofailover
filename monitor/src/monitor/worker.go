package monitor

import (
	"context"
	"github.com/jackc/pgconn"
	"log"
	"time"
)

func (node *AutoNode)connect_db() (*pgconn.PgConn, error) {
	// TODO:
	return nil, nil
}
func start_worker(node *AutoNode, resChan chan ResponseMessage)  {

	log.Println("Start worker loop for ", node)
	var err error
	var cmdMessage CommandMessage
	var resMessage ResponseMessage

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
		node.T.Reset(wait_time)
	waitFor:
		for {
			select {
			case cmdMessage = <- node.cmdMessage:
				{
					assert(cmdMessage.owner == nil || cmdMessage.owner == node)
					timeout = false
					if !node.T.Stop() {
						<-node.T.C
					}
					if cmdMessage.delay == 0 {
						log.Println("receive ", cmdMessage.messageType, " delay is 0")
						break waitFor
					}
					node.T.Reset(time.Millisecond * cmdMessage.delay)
				}
			case <- node.T.C:
				log.Println("worker timeout ")
				break waitFor

			}
		}
		if timeout {
			// ai is invalid, run default collect
			log.Println("Time, the group doesn't send the command to this node:", node)
			cmdMessage.messageType = MSG_STATUS
			cmdMessage.owner = nil
		}
		if resMessage.messageType == MSG_EXIT {
			node.quit = true
			// FIXME: shall we do come cleanup?
			//  1. close the channel for command message
			//  2. close the database connection
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
		log.Println(err)
		//sql := fmt.Sprintf("select * from autofailover_execute('status', '%c')", state.role)
		//sql = buildSQL(&ai, node.state.role)
		//result:=state.conn.ExecParams(context.Background(), sql, nil, nil, nil,nil,).Read()
		//if result.Err != nil {
		//	log.Println(result.Err)
		//	wait_time = COLLECTOR_RETRY_TIMEOUT
		//	err_cnt++
		//	err = state.conn.Close(context.Background())
		//	if err != nil {
		//		log.Println("Close pgconn:", err)
		//	}
		//	state.conn = nil
		//	continue
		//}
		//if len(result.Rows) != 1 || len(result.Rows[0]) != 5 {
		//	log.Println("unexpected data form: collect_status: ", result.Rows)
		//	wait_time = COLLECTOR_RETRY_TIMEOUT
		//	err_cnt++
		//	continue
		//}
		//status.role = result.Rows[0][0][0]
		//status.syncrep = string(result.Rows[0][1])
		//status.sync_state = string(result.Rows[0][2])
		//status.lsn = string(result.Rows[0][3])
		//status.walconn = result.Rows[0][4][0] == 't'
		//status.time_updated = time.Now()
		//
		//
		//log.Printf("received status: role='%c' syncrep='%s' sync_state='%s' lsn=%s walconn='%s'\n",
		//	status.role, status.syncrep, status.sync_state, status.lsn, string(result.Rows[0][4]))
		//
		//ch <- srm
		resChan <- resMessage
		err_cnt = 0
		wait_time = COLLECTOR_IDLE_TIMEOUT * 10
	}

}
func (node *AutoNode)processMessage(cmdMessage *CommandMessage, resMessage *ResponseMessage) error {
	var sql string
	switch cmdMessage.messageType {
	case MSG_STATUS: sql = "select * from autofaiover_execute('status')"
	case MSG_UNSYNCREP: sql = "select * from autofaiover_execute('unsyncrep')"
	case MSG_SYNCREP: sql = "select * from autofaiover_execute('syncrep')"
	case MSG_PROMOTE: sql = "select * from autofaiover_execute('promote')"
	case MSG_WRITE_QUERY:
		if node._has_temp_table {
			sql = "insert into autofailover_write_temp_table values(1);"
		} else {
			sql = "create temp table autofailover_write_temp_table as select 1 i;"
		}
		sql += "select * from autofaiover_execute('status');"
	default:
		log.Fatalln("unknown command from group: ", cmdMessage)
	}

	result:=node.conn.ExecParams(context.Background(), sql, nil, nil, nil,nil,).Read()

	if result.Err != nil {
		log.Println(result.Err)
		resMessage.returnCode = MRC_ERR_EXECUTE_SQL
		return result.Err
	}

	if len(result.Rows) != 1 || len(result.Rows[0]) != 5 {
		log.Println("unexpected data form: collect_status: ", result.Rows)
		resMessage.returnCode = MRC_ERR_SIZE
		return MRC_ERR_SIZE
	}

	return nil
}