package monitor

import "time"

func mockPrimary() *AutoNode {
	now := time.Now()
	node := AutoNode{
		ID: 1,
		host: "localhost",
		pgdata: "/Users/hawu/workspace/postgresql/datadirs/data-m",
		port: 5432,
		role: 'p',
		valid: false,
		quit: false,
		_has_temp_table:false,

		time_updated:now,
		time_walconn_updated:now,

		cmdMessage:make(chan CommandMessage),
		T: time.NewTimer(time.Hour),

	}
	node.T.Stop()

	return &node
}
func mockSecondary() *AutoNode {
	now := time.Now()
	node := AutoNode{
		ID: 2,
		host: "localhost",
		pgdata: "/Users/hawu/workspace/postgresql/datadirs/data-m",
		port: 5433,
		valid: false,
		quit: false,
		role:'s',
		_has_temp_table:false,

		time_updated:now,
		time_walconn_updated:now,

		cmdMessage:make(chan CommandMessage),
		T: time.NewTimer(time.Hour),

	}
	node.T.Stop()

	return &node
}

func MockGroupSingle() *AutoGroup {
	g := new(AutoGroup)
	node0 := mockPrimary()
	node0.update = node_single
	g.active_nodes = append(g.active_nodes, node0)

	g.target = node0
	g.targetID = node0.ID
	g.running_state = GRS_SINGLE
	g.sync_state = GSS_UNKNOWN
	g.resMessageChan = make(chan ResponseMessage, 1)

	return g
}

func MockGroupNormal() *AutoGroup {
	g := new(AutoGroup)
	node0 := mockPrimary()
	node1 := mockSecondary()
	node0.update = primary_normal
	node1.update = secondary_normal
	g.active_nodes = append(g.active_nodes, node0, node1)

	g.target = node0
	g.targetID = node0.ID
	g.running_state = GRS_NORMAL
	g.sync_state = GSS_UNKNOWN
	g.resMessageChan = make(chan ResponseMessage, 1)

	return g
}
