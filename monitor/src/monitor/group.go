package monitor

import (
	"context"
	"fmt"
	"github.com/jackc/pgconn"
	"log"
	"time"
)

const (
	G_STATE_INIT0 byte = '0'
	G_STATE_SINGLE = 's'
	G_STATE_NORMAL = 'n'
	G_STATE_PROMOTING = 'p'
)

func (node *auto_node)connect() (*pgconn.PgConn, error) {
	conn_info := fmt.Sprintf("user=hawu host=%s port=%d dbname=postgres", node.host, node.port)
	return pgconn.Connect(context.Background(), conn_info)
}

// receives commands from the group manager, run request and returns result
// through a channel owned by the node
// sends the result to the group manager through a shared channel owned by
// the group manager
const COLLECTOR_RETRY_TIMEOUT = time.Millisecond * 1000
const COLLECTOR_IDLE_TIMEOUT  = time.Hour * 24

func buildSQL(ai *ActionItem, last_role byte) string {
	switch ai.CMD {
	default:
		fallthrough
	case COLLECT_STATUS: return fmt.Sprintf("select * from autofailover_execute('status', '%c')", last_role)
	case SYNC_REP: return fmt.Sprintf("select * from autofailover_execute('syncrep', '%c')", last_role)
	case UNSYNC_REP: return fmt.Sprintf("select * from autofailover_execute('unsyncrep', '%c')", last_role)
	case PROMOTE: return "select * from autofailover_execute('promote', 's')"
	}
}
func (node *auto_node)start_loop(ch chan <- ServerResponseMessage) {
	log.Println("Start worker loop for ", node.name)
	state := node.state
	if state == nil {
		// init state
		now := time.Now()
		state = &auto_node_state{
			node_status {
				role:       '?',
				walconn:    false,
				sync_state: "", // may load from disk
				lsn:        "",
				time_updated:  now,
				time_disconnect:now,
			},
			nil,
		}
	}
	var err error
	var srm ServerResponseMessage
	var ai ActionItem
	var status node_status
	srm.self = node
	srm.status = &status

	wait_time := COLLECTOR_IDLE_TIMEOUT

	err_cnt := 0
	for {
		timeout := true
		if err_cnt > 3 {
			srm.CMD = ERR_RES | ai.CMD
			ch <- srm
			err_cnt = 0
		}
		node.state.T.Reset(wait_time)
		waitFor:
		for {
			select {
			case ai = <- node.state.cmd_chan:
				{
					timeout = false
					if !node.state.T.Stop() {
						<-node.state.T.C
					}
					if ai.delay == 0 {
						log.Println("receive ", ai.CMD, " delay is 0")
						break waitFor
					}
					node.state.T.Reset(time.Millisecond * time.Duration(ai.delay))
				}
			case <- node.state.T.C:
				log.Println("worker timeout ")
				break waitFor

			}
		}
		if timeout {
			// ai is invalid, run default collect
			ai.CMD = COLLECT_STATUS
			ai.target = node
		}
		assert(ai.target == node)


		srm.CMD = ai.CMD
		if state.conn == nil {
			conn, err := node.connect()
			if err != nil {
				log.Println(err)
				wait_time = COLLECTOR_RETRY_TIMEOUT
				err_cnt++
				continue
			}
			state.conn = conn
		}
		sql := fmt.Sprintf("select * from autofailover_execute('status', '%c')", state.role)
		sql = buildSQL(&ai, node.state.role)
		result:=state.conn.ExecParams(context.Background(), sql, nil, nil, nil,nil,).Read()
		if result.Err != nil {
			log.Println(result.Err)
			wait_time = COLLECTOR_RETRY_TIMEOUT
			err_cnt++
			err = state.conn.Close(context.Background())
			if err != nil {
				log.Println("Close pgconn:", err)
			}
			state.conn = nil
			continue
		}
		if len(result.Rows) != 1 || len(result.Rows[0]) != 5 {
			log.Println("unexpected data form: collect_status: ", result.Rows)
			wait_time = COLLECTOR_RETRY_TIMEOUT
			err_cnt++
			continue
		}
		status.role = result.Rows[0][0][0]
		status.syncrep = string(result.Rows[0][1])
		status.sync_state = string(result.Rows[0][2])
		status.lsn = string(result.Rows[0][3])
		status.walconn = result.Rows[0][4][0] == 't'
		status.time_updated = time.Now()


		log.Printf("received status: role='%c' syncrep='%s' sync_state='%s' lsn=%s walconn='%s'\n",
			status.role, status.syncrep, status.sync_state, status.lsn, string(result.Rows[0][4]))

		ch <- srm
		err_cnt = 0
		wait_time = COLLECTOR_IDLE_TIMEOUT * 10
	}

}


type GroupManageCMD byte
type GroupManageMessage struct {
	CMD GroupManageCMD
	data interface{}
}
const (
	ADD_NODE GroupManageCMD = iota + 1
	DEL_NODE
	SHUTDOWN
)


// messages from worker to group manager
// the worker sends the server response to the group manager
type ServerCMD byte

const (
	COLLECT_STATUS ServerCMD = 'c'
	INITIAL_INFO = 'I'
	SYNC_REP = 's'
	UNSYNC_REP ='u'
	PROMOTE='p'
	ERR_RES = 0x80 // tells the group that action failed
)
type ServerResponseMessage struct {
	CMD ServerCMD
	self *auto_node
	status *node_status
}
type ActionItem struct {
	CMD ServerCMD
	delay int // 0 now, >0 delay in ms
	target *auto_node
}

func (node *auto_node)update_status(status *node_status)  {
	p := &node.state.node_status

	p.role = status.role
	p.lsn = status.lsn
	p.health = true
	p.syncrep = status.syncrep
	log.Printf("update status node: %p, '%s', '%s'\n", node, node.name, p.syncrep)
	p.time_updated = status.time_updated // time was set in collector
	if p.walconn || status.walconn {
		p.time_disconnect = status.time_updated
	}
	p.walconn = status.walconn
	p.sync_state = status.sync_state
}

func (g *auto_group)run_state_init0(gmm <- chan GroupManageMessage) {
	msg, ok := <- gmm
	if !ok {
		// shutdown
	}
	switch msg.CMD {
	case ADD_NODE: {
		node := msg.data.(*auto_node)
		// TODO: add node
		log.Println(node)
	}
	case DEL_NODE: {
		// TODO: del node
		name := msg.data.(string)
		log.Println(name)
	}
	}

}

func (g *auto_group)run_state_single(srm *ServerResponseMessage, ai *ActionItem) {
	log.Println("Start single loop")
	switch srm.CMD &^ ERR_RES {
	case INITIAL_INFO: {
		// TODO:
	}
	case COLLECT_STATUS, SYNC_REP, UNSYNC_REP, PROMOTE: {
		if srm.CMD & ERR_RES == 0 {
			srm.self.update_status(srm.status)
		}
		if srm.self.state.role != 'p' {
			// only one node
			// if not primary, promote
			ai.CMD = PROMOTE
			ai.delay = 200
			log.Println("promote node ", srm.self.name)
		} else if srm.self.state.syncrep != "" {
			ai.CMD = UNSYNC_REP
			ai.delay = 500
			log.Println("sync off node ", srm.self.name)
		} else {
			ai.CMD = COLLECT_STATUS
			ai.delay = 10 * 1000
		}
		ai.target = srm.self
	}
	default:
		log.Fatalln("invalid server response message:", srm.CMD)
	}
}

func (g *auto_group)find_pair() (primary, secondary *auto_node) {
	for _, node := range g.nodes {
		switch node.state.role {
		case 'p': {
			if primary != nil {
				log.Fatalln("found two primary in g.nodes")
			}
			primary = node
		}
		case 's': {
			secondary = node
		}
		}
	}
	if primary == nil {
		log.Fatalln("Found no primary in g.nodes")
	}
	if secondary == nil {
		log.Fatalln("Found no secondary in g.nodes")
	}
	return
}
const TIMEOUT_PROMOTE = time.Second * 10
const TIMEOUT_UNSYNCRERP = time.Second * 5
const DEFAULT_COLLECT_INTERVAL  = 2000
const PROMOTING_RETRY_INTERVAL = 3 * time.Second


func (g *auto_group)run_state_normal(srm *ServerResponseMessage, ai *ActionItem) {
	primary := g.target
	from_primary := primary == srm.self

	ai.CMD = COLLECT_STATUS
	ai.target = srm.self
	if from_primary {
		ai.delay = 2000 // idle period
	} else {
		ai.delay = 30 * 1000 // idle period
	}
	log.Printf("recv cmd = %X %c\n", byte(srm.CMD), byte(srm.CMD))
	switch srm.CMD {
	case UNSYNC_REP, SYNC_REP, COLLECT_STATUS, PROMOTE:
			srm.self.update_status(srm.status)
	default:
		log.Println("unknown response message: ", srm.CMD)
		ai.delay = 0
		return
	}

	st := primary.state
	now := time.Now()

	if now.Sub(st.time_updated) >= TIMEOUT_PROMOTE {
		// primary in inactive recently, considerinig promotion
		// primary is inactive for too long, start promotion
		target := g.choose_promotion_candidate()
		if primary.is_safe_promote() && target.state.health {
			// the response must be from the secondary
			ai.CMD = PROMOTE
			ai.delay = 0
			ai.target = target
			g.state = G_STATE_PROMOTING
			g.target = target
			// TODO: log promotion
			log.Println("Promote for node ", target.name)
		}
	} else if st.health {
		// primary is active recently, considering update states: syncrep, walconn
		if st.walconn {
			// may need turn on syncrep if the last status if off and the lsn is close enough
			if from_primary && st.syncrep != "*" {
				ai.CMD = SYNC_REP
				ai.delay = 500
				ai.target = primary
				// keep the syncrep unsafe to promote
				log.Printf("primary turn on syncrep, %p '%s'\n", primary, st.syncrep)
			}
		} else if now.Sub(st.time_disconnect) > TIMEOUT_UNSYNCRERP {
			// walconn disconnects for a long time, try synoff
			if from_primary && st.syncrep != "" {
				// TODO: log unsyncrep
				st.syncrep = "" // prevent promoting the secondary from now on.
				ai.CMD = UNSYNC_REP
				ai.delay = 0
				ai.target = primary
			}
		}
	} else {
		// promotion is not timeout && primary is not healthy
		// we wait until promotion or primary is healthy.
	}
}

func (g *auto_group)choose_promotion_candidate() *auto_node {
	for _, node := range g.nodes {
		if node.state.role == 's' {
			return node
		}
	}
	log.Fatalln("no candidate for promotion")
	return nil
}
func (node *auto_node)is_safe_promote() bool {
	return node.state.syncrep=="*" && node.state.sync_state=="streaming"
}

func (g *auto_group)run_state_promoting(srm *ServerResponseMessage, ai *ActionItem)  {
	target := g.target
	from_target := target == srm.self

	if srm.CMD & ERR_RES == 0 {
		srm.self.update_status(srm.status)
	}
	if !from_target {
		ai.CMD = COLLECT_STATUS
		ai.target = srm.self
		ai.delay = 5000
		return
	}
	st := target.state
	now := time.Now()
	if st.role == 'p' {
		// finished promoting
		log.Println("finished promoting")
		ai.CMD = COLLECT_STATUS
		ai.target = target
		ai.delay = 0
		if len(g.nodes) == 1 {
			g.state = G_STATE_SINGLE
		} else {
			// fixme: demoted dead node
			g.state = G_STATE_NORMAL
		}
		g.wal_sync = '?'
	} else if now.Sub(g.promoting_time) > PROMOTING_RETRY_INTERVAL {
		// retry promoting
		ai.CMD = PROMOTE
		ai.target = target
		ai.delay = 0
	}
}

func (g *auto_group)init_loop() {
	var ai ActionItem
	ai.CMD = COLLECT_STATUS
	ai.delay = 0
	for _, node := range g.nodes {
		go node.start_loop(g.resMessage)
		log.Println("Start collect status for node - ", node.name)
		ai.target = node
		node.state.cmd_chan <- ai
	}
	// start by sending collect status

}
func (g *auto_group)run_loop()  {
	var ai ActionItem
	var srm ServerResponseMessage

	g.init_loop()

	quit := false
	mainFor:
	for !quit {
		select {
		case quit = <- g.quit:
			quit = true
			break mainFor
		case srm = <-g.resMessage:
		}

		sanity_check(g, &srm)
		switch g.state {
		// ignore init0
		case G_STATE_INIT0: g.run_state_init0(nil)
		case G_STATE_SINGLE: g.run_state_single(&srm, &ai)
		case G_STATE_NORMAL: g.run_state_normal(&srm, &ai)
		case G_STATE_PROMOTING: g.run_state_promoting(&srm, &ai)
		}
		ai.target.state.cmd_chan <- ai
		log.Println("send command to worker, ", ai.delay)
	}
}
//
/////////////////////////////////////////////////////////////////////////////////
///// 						NEW DESIGN										///
/////////////////////////////////////////////////////////////////////////////////
//
//type AutoNode struct {
//
//	role byte
//	time_updated time.Time
//	time_walconn_disconnected time.Time
//	fn func()
//}
//
//func (node *AutoNode)syncrep() bool {
//	return false
//}
//func (node *AutoNode)walconn() bool {
//	return false
//}
//func (node *AutoNode)lastCMD() int {
//	return 0
//}
//func (node *AutoNode)should_sync_off() bool {
//	return false
//}
//type GroupSyncState int
//const (
//	GS_UNKNOWN GroupSyncState = iota + 1
//	GS_UNSYNC  // primary has turned off syncrep, needs to turn it on first.
//	GS_P_SYNC // primary has turned on syncrep, needs to write query.
//	GS_READY // received write query, group is ready for promotion.
//)
//type GroupRunningState int
//const (
//	GRS_INIT0 GroupRunningState = iota + 1
//	GRS_SINGLE
//	GRS_NORMAL
//	GRS_PROMOTING
//)
//type AutoGroup struct {
//	target *AutoNode
//
//	active_nodes []*AutoNode
//	demoted_nodes []*AutoNode
//	// persistent fields
//	running_state GroupRunningState
//	sync_state GroupSyncState
//}
//
//func (g *AutoGroup)save()  {
//
//}
//
//func node_single(g *AutoGroup, node *AutoNode, resMessage interface{})  {
//	// do checks, update, actions
//	// sync => unsync
//	// promote
//
//	/// checks
//	if node.role != 'p' {
//		// TODO: not primary, do promote
//
//	} else if node.syncrep() {
//		// sync => unsync
//	}
//}
//
//func primary_normal(g *AutoGroup, node *AutoNode, resMessage interface{})  {
//	// sync off
//	// sync on
//	// write q
//	if !node.syncrep() {
//		// unsync => sync
//		if node.walconn() {
//			// TODO: turn syncrep on
//		} else {
//			// not syncrep and walconn is off
//			// do nothing
//		}
//	} else {
//		// sync => {unsync | write & ready}
//		if node.time_walconn_disconnected == node.time_updated {
//			if g.sync_state != GS_READY {
//				// try to send write query and become ready for promotion
//				// TODO: if received command is write-query
//				if node.lastCMD() == 1 {
//					g.sync_state = GS_READY
//					g.save()
//				} else {
//					// if last command is not write query, send it
//					// TODO: send write-query
//				}
//			}
//		} else if node.should_sync_off() { // replication is not established in last cycle, considering sync off
//			if g.sync_state == GS_P_SYNC || g.sync_state == GS_READY {
//				g.sync_state = GS_UNSYNC
//				g.save()
//			}
//			// TODO: turn syncrep off
//		}
//	}
//}
//
//func secondary_normal(g *AutoGroup, node *AutoNode, resMessage interface{})  {
//	// do checks and updates, no action
//}
//func demoted_all_state(g *AutoGroup, node *AutoNode, resMessage interface{})  {
//	// should not receive messages
//}
//
//func target_promoting(g *AutoGroup, node *AutoNode, resMessage interface{})  {
//	assert(node.role != 'p')
//
//
//}
