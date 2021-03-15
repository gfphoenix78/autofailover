package monitor

import "time"

var node_infos = [2]auto_node {
	{
		"data-m",
		"localhost",
		"/Users/hawu/workspace/postgresql/datadirs/data-m",
		5432,
		nil,
	},
	{
		"data-s",
		"localhost",
		"/Users/hawu/workspace/postgresql/datadirs/data-s",
		5433,
		nil,
	},
}

func mock_primary() *auto_node {
	now := time.Now()
	node := &node_infos[0]
	status := node_status{
		role: '?',
		walconn: false,
		health:false,
		syncrep: "",
		sync_state: "",
		lsn: "",
		time_updated: now,
		time_disconnect:now,
		cmd_chan:make(chan ActionItem, 1),
		T: time.NewTimer(time.Hour * 1000000),
		quit: false,
	}
	status.T.Stop()

	state := auto_node_state{
		node_status: status,
	}
	node.state = &state
	return node
}
func mock_secondary() *auto_node {
	now := time.Now()
	node := &node_infos[1]
	status := node_status{
		role: '?',
		walconn: false,
		health:false,
		syncrep: "",
		sync_state: "",
		lsn: "",
		time_updated: now,
		time_disconnect:now,
		cmd_chan:make(chan ActionItem, 1),
		T: time.NewTimer(time.Hour * 1000000),
		quit: false,
	}
	status.T.Stop()
	state := auto_node_state{
		node_status: status,
	}
	node.state = &state
	return node
}

func buildGroupSingle() *auto_group {
	g := new(auto_group)
	node0 := mock_primary()
	//node1 := mock_secondary()
	g.nodes = []*auto_node {node0}

	g.ID = 1
	g.sysid = ""
	g.state = G_STATE_SINGLE
	g.target = node0
	g.wal_sync = '?'
	g.quit = make(chan bool)
	g.resMessage = make(chan ServerResponseMessage, 1)

	return g
}

func buildGroupNormal() *auto_group {
	g := new(auto_group)
	node0 := mock_primary()
	node1 := mock_secondary()
	g.nodes = []*auto_node {node0, node1}

	g.ID = 1
	g.sysid = ""
	g.state = G_STATE_NORMAL
	g.target = node0
	g.wal_sync = '?'
	g.quit = make(chan bool)
	g.resMessage = make(chan ServerResponseMessage, 1)

	return g
}
