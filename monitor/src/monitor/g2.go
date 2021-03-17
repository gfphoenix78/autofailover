package monitor

import (
	"fmt"
	"github.com/jackc/pgconn"
	"log"
	"time"
)

///////////////////////////////////////////////////////////////////////////////
/// 						NEW DESIGN										///
///////////////////////////////////////////////////////////////////////////////
type messageType byte
type returnCode byte
const (
	MSG_NOOP 	messageType = iota
	MSG_STATUS
	MSG_SYNCREP
	MSG_UNSYNCREP
	MSG_PROMOTE
	MSG_WRITE_QUERY
	MSG_EXIT // order the work to exit the goroutine
)

// message return code
const (
	MRC_OK returnCode = iota
	MRC_ERROR
	MRC_ERR_EXECUTE_SQL
	MRC_ERR_SIZE
)

func (mt messageType)String() string {
	names := [...]string {
		"NOOP",
		"STATUS",
		"SYNCREP",
		"UNSYNCREP",
		"PROMOTE",
		"WRITE_QUERY",
		"EXIT",
	}
	if int(mt) < len(names) {
		return names[mt]
	}
	return "UNKNOWN"
}

func (rc returnCode)Error() string {
	return rc.name()
}
func (rc returnCode)String() string {
	return rc.name()
}
func (rc returnCode)name() string {
	messages := [...]string {
		"OK",
		"General Error",
		"Error: execute error",
		"Error: wrong size",
	}
	if int(rc) < len(messages) {
		return messages[rc]
	}
	return "<UNKNOWN Error Code>"
}

func (rc returnCode)OK() bool {
	return rc == MRC_OK
}
// sent by channel,
type ResponseMessage struct {
	owner *AutoNode // who sends the message

	messageType
	returnCode

	role byte	 // 'p' or 's'
	walconn bool // true if the walconn is alive
	syncrep string
}

type CommandMessage struct {
	messageType
	delay time.Duration
	// private
	//owner *AutoNode
	//sql string
}
type AutoNode struct {
	/// unique reference to the node
	ID int
	/// meta data for connection
	host string
	pgdata string
	port uint16

	valid bool // status has been updated
	quit bool  // to indicate the node should exit
	role byte
	walconn bool
	syncrep string

	_has_temp_table bool // for write query
	time_updated time.Time
	time_walconn_updated time.Time

	cmdMessage chan CommandMessage // receive command from the group
	T *time.Timer
	conn *pgconn.PgConn
	update func(g *AutoGroup, node *AutoNode, resMessage  *ResponseMessage, cmd *CommandMessage)
}

func (node *AutoNode)String() string {
	return fmt.Sprintf("node-%d", node.ID)
}

//func (node *AutoNode)syncrep() bool {
//	return node.syncrep_ == '*'
//}

func (node *AutoNode)lastCMD() int {
	return 0
}
func (node *AutoNode)should_sync_off() bool {
	// walconn is false and it disconnects for some time
	return !node.walconn && node.time_walconn_updated.Before(node.time_updated)
}
type GroupSyncState byte
const (
	GSS_UNKNOWN  GroupSyncState = iota
	GSS_P_UNSYNC  // primary has turned off syncrep, needs to turn it on first.
	GSS_P_SYNC    // primary has turned on syncrep, needs to write query.
	GSS_G_SYNC    // received write query, group is ready for promotion.
)
type GroupRunningState int
const (
	GRS_INIT0 GroupRunningState = iota
	GRS_SINGLE
	GRS_NORMAL
	GRS_PROMOTING
)

func (gss GroupSyncState)String() string {
	names := [...]string{
		"UNKNOWN",
		"P_UNSYNC",
		"P_SYNC",
		"G_SYNC",
	}
	return names[gss]
}
func (grs GroupRunningState) String() string {
	names := [...]string {
		"INIT0",
		"SINGLE",
		"NORMAL",
		"PROMOTING",
	}
	return names[grs]
}

type AutoGroup struct {
	target *AutoNode

	resMessageChan chan ResponseMessage

	active_nodes []*AutoNode
	demoted_nodes []*AutoNode
	// persistent fields
	targetID int // TODO: better type than int?
	running_state GroupRunningState
	sync_state GroupSyncState
}

func (g *AutoGroup)save()  {

}
func (g *AutoGroup)should_promote() (*AutoNode, bool) {
	x := g.target
	assert(x != nil)
	now := time.Now()
	if now.Sub(x.time_updated) > 10 * time.Second {
		for _, node := range g.active_nodes {
			if x == node {
				continue
			}
			assert(node.role != 'p')
			if now.Sub(node.time_updated) < 3*time.Second &&
				node.time_updated.Sub(node.time_walconn_updated) > 3*time.Second {
				return node, true
			}
		}
	}
	return nil, false
}
func (g *AutoGroup)prepare_promote1(node *AutoNode, cmd *CommandMessage) {
	assert(g.target == node)
	assert(g.targetID == node.ID)
	g.running_state = GRS_PROMOTING
	g.sync_state = GSS_UNKNOWN
	for _, xx := range g.active_nodes {
		xx.update = secondary_promoting
	}

	node.role = 'S'
	node.update = target_promoting

	g.save()
}

func (g *AutoGroup)demote_node_from_active_list(node *AutoNode) {
	assert(g.target == node)
	index := -1
	for idx, xx := range g.active_nodes {
		if xx == node {
			assert(xx.role == 'p')
			index = idx
			break
		}
	}
	assert(index >= 0)
	g.active_nodes = append(g.active_nodes[:index], g.active_nodes[index+1:]...)
	g.demoted_nodes = append(g.demoted_nodes, node)
	g.target = nil
	node.role = 'd'
}
func (g *AutoGroup)prepare_promote_normal(node *AutoNode, cmd *CommandMessage) *AutoNode {
	assert(g.target != node)
	// remove target from active & add it to the demoted array
	p := g.target
	g.demote_node_from_active_list(p)
	p.role = 'd'
	p.update = demoted_all_state

	g.target = node
	g.targetID = node.ID
	g.running_state = GRS_PROMOTING
	for _, xx := range g.active_nodes {
		xx.update = secondary_promoting
	}

	node.role = 'S'
	node.update = target_promoting

	g.save()

	return p
}
func node_single(g *AutoGroup, node *AutoNode, resMessage *ResponseMessage, cmd *CommandMessage)  {
	// do checks, update, actions
	// sync => unsync
	// promote
	debug4("run node_single...")
	role := byte('?')
	now := time.Now()
	if !resMessage.OK() {
		log.Printf("failed to run command '%s' from node %s, %s\n",
			resMessage.messageType, node.String(), resMessage.returnCode)

		if !node.valid {
			cmd.messageType = MSG_STATUS
			cmd.delay = 3 * time.Second
			return
		}
		role = node.role
	} else {
		role = resMessage.role
		node.syncrep = resMessage.syncrep
		node.walconn = resMessage.walconn
		node.time_updated = now
		if !node.valid || node.walconn {
			node.time_walconn_updated = now
		}
	}

	if role != 'p' {
		g.prepare_promote1(node, cmd)
	} else if node.syncrep == "*" {
		cmd.messageType = MSG_UNSYNCREP
		cmd.delay = 0
	} else {
		// nothing
		cmd.messageType = MSG_STATUS
		cmd.delay = 3 * time.Second
	}
}

func primary_normal(g *AutoGroup, node *AutoNode, resMessage  *ResponseMessage, cmd *CommandMessage)  {
	// sync off
	// sync on
	// write q
	cmd.messageType = MSG_STATUS
	cmd.delay = 3 * time.Second
debug4("primary_normal...")
	if !resMessage.OK() {
		log.Printf("failed to run command '%s' from node %s, %s\n",
					resMessage.messageType, node.String(), resMessage.returnCode)

		return
	}
	if resMessage.role != 'p' {
		log.Println("expected primary, but the report role is secondary.")
		cmd.messageType = MSG_STATUS
		cmd.delay = 5 * time.Second
		return
	}
	now := time.Now()
	if !node.valid {
		assert(resMessage.messageType == MSG_STATUS)
		node.role = resMessage.role
		node.time_walconn_updated = now
		node.valid = true
	}

	//node.role = resMessage.role // should always the same value
	node.syncrep = resMessage.syncrep
	node.walconn = resMessage.walconn
	node.time_updated = now
	// time_walconn_disconnected is update if current walconn is up
	if node.walconn {
		node.time_walconn_updated = now
	}
	log.Println("node.time_walconn_updated:", node.time_walconn_updated)
	if node.syncrep == "" {
		// unsync => sync
		if node.walconn {
			cmd.messageType = MSG_SYNCREP
			cmd.delay = 0
		} else {
			// not syncrep and walconn is off
			// do nothing
		}
	} else {
		// sync => {unsync | write & ready}
		if node.time_walconn_updated == node.time_updated && node.walconn {
			if g.sync_state != GSS_G_SYNC {
				// try to send write query and become ready for promotion
				if resMessage.messageType == MSG_WRITE_QUERY {
					g.sync_state = GSS_G_SYNC
					g.save()
				} else {
					// if last command is not write query, send it
					cmd.messageType = MSG_WRITE_QUERY
					cmd.delay = 0
				}
			}
		} else if node.should_sync_off() { // replication is not established in last cycle, considering sync off
			if g.sync_state == GSS_P_SYNC || g.sync_state == GSS_G_SYNC {
				g.sync_state = GSS_P_UNSYNC
				g.save()
			}
			cmd.messageType = MSG_UNSYNCREP
			cmd.delay = 0
		}
	}
}

func secondary_normal(g *AutoGroup, node *AutoNode, resMessage  *ResponseMessage, cmd *CommandMessage)  {
	// do checks and updates, no action
	cmd.messageType = MSG_STATUS
	cmd.delay = 3 * time.Second // default interval to collect status from the secondary
	debug4("secondary_normal: role=%v, syncrep=%v walconn=%v",
		resMessage.role,
		resMessage.syncrep,
		resMessage.walconn)
	if !resMessage.OK() {
		log.Printf("failed to run command '%s' from node %s, %s\n",
			resMessage.messageType, node.String(), resMessage.returnCode)
		//cmd.delay = 3 * time.Second
		return
	}
	if resMessage.role != 's' {
		log.Println("expected secondary, but the report role is secondary.")
		cmd.delay = 5 * time.Second
		return
	}
	now := time.Now()
	if !node.valid {
		assert(resMessage.messageType == MSG_STATUS)
		node.role = resMessage.role
		node.time_walconn_updated = now
		node.valid = true
	}

	//node.role = resMessage.role // should always the same value
	node.syncrep = resMessage.syncrep
	node.walconn = resMessage.walconn
	node.time_updated = now
	// time_walconn_disconnected is update if current walconn is up
	if node.walconn {
		node.time_walconn_updated = now
	}
	
	// check promote
	assert(g.target != nil)
	log.Println(g.target, g.target.role)
	assert(g.target.role == 'p')
	if x, ok := g.should_promote(); ok && x == node {
		// promote x
		// demote p.target
		g.prepare_promote_normal(x, cmd)
		cmd.messageType = MSG_PROMOTE
		cmd.delay = 0

		g.save()
	}
}
func demoted_all_state(g *AutoGroup, node *AutoNode, resMessage  *ResponseMessage, cmd *CommandMessage)  {
	// should not receive messages
	log.Println("demoted node in a group, ignore")
	cmd.messageType = MSG_EXIT
	cmd.delay = 0
}

func target_promoting(g *AutoGroup, node *AutoNode, resMessage  *ResponseMessage, cmd *CommandMessage)  {
	cmd.messageType = MSG_PROMOTE
	cmd.delay = 3 * time.Second

	assert(g.target == node)
	assert(node.role == 'S')

	if !resMessage.OK() {
		// bad message
		log.Printf("Can't run command '%s' for node '%s', '%s'\n",
			resMessage.messageType, node.String(), resMessage.returnCode)
		return
	}
	now := time.Now()
	if !node.valid {
		assert(resMessage.messageType == MSG_STATUS || resMessage.messageType == MSG_PROMOTE)
		node.valid = true
		node.time_walconn_updated = now
	}
	node.syncrep = resMessage.syncrep
	node.walconn = resMessage.walconn
	node.time_updated = now
	if node.walconn {
		node.time_walconn_updated = now
	}

	if resMessage.role == 'p' {
		// finish promotion
		node.role = 'p'
		if node.syncrep == "*" {
			g.sync_state = GSS_P_SYNC
		} else {
			g.sync_state = GSS_P_UNSYNC
		}
		switch len(g.active_nodes) {
		case 1: {
			g.running_state = GRS_SINGLE
			node.update = node_single
		}
		case 0:
			// what the hell
			panic("No active nodes, WTF")
		default:
			g.running_state = GRS_NORMAL
			for _, xx := range g.active_nodes {
				assert((xx.role == 'p' && xx == node) || xx.role == 's')
				xx.update = secondary_normal
			}
			node.update = primary_normal
		}
		g.save()
		cmd.messageType = MSG_STATUS
		cmd.delay = 0
	} else {
		// not promoted, do the default command
	}
}
func secondary_promoting(g *AutoGroup, node *AutoNode, resMessage  *ResponseMessage, cmd *CommandMessage)  {
	log.Println("more secondaries in a group, ignore")
}

func (g *AutoGroup)init_loop() {
	cmd := CommandMessage{
		messageType: MSG_STATUS,
		delay:0,
	}
	for i, n := 0, len(g.active_nodes); i<n; i++ {
		x := g.active_nodes[i]
		go start_worker(x, g.resMessageChan)
		x.cmdMessage <- cmd
	}
}

func (g *AutoGroup)Loop()  {

	var cmdMessage CommandMessage
	var resMessage ResponseMessage

	g.init_loop()

	for {
		resMessage = <-g.resMessageChan
		sender := resMessage.owner
		assert(sender != nil)
		sanity_check(g, &resMessage)
		sender.update(g, sender, &resMessage, &cmdMessage)
		log.Println(sender, cmdMessage.messageType, cmdMessage.delay)
		sender.cmdMessage <- cmdMessage

		log.Printf("%s role='%c', syncrep='%s', walconn='%t'\n",
					sender, sender.role, sender.syncrep, sender.walconn)
		log.Printf("target='%s', sync state='%s', len(nodes)=%d\n",
					g.target, g.sync_state, len(g.active_nodes))
	}
}