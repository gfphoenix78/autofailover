g.state = {**init0** | **single** | **normal** | **promoting**}

	init0: empty group, has no node
	single: the group has only one node
	normal: the group has one primary and is running well
	promoting: one node is choosen to be promoted as new primary

g.target: points to the candidate primary node

	case init0: null
	case single: the node in group
	case normal: the primary in group
	case promoting: the node to being promoted

g.sync = {**unknown** | **primary_sync** | **primary_unsync** | **group_sync**}

	unknown: don't any state, need to ensure that primary is syncrep
	primary_sync: primary is syncrep, needs to verify more
	primary_unsync: primary is unsyncrep
	group_sync: primary is syncrep and group is ready for promotion

`group_state_single()`:

	// set default command: collect status
	// candidate commands:
	//	1. turn off syncrep
	//	2. promote (the single node is secondary)
	if node.role is 's':
		store: g.state = promoting
				g.target = node
				g.sync = unknown // not used in/after promoting
		// send promoting command
	else:
		if node_syncrep:
			// send unsync command

`group_state_proting()`:

	// receive response message from the server
	// check if the target is primary
	if role is 'p':
		// finish promoting
		switch len(g.nodes):
			case 0:
				g.state = init0
				g.target = null
				g.sync = unknown
			case 1:
				g.state = single
				g.target = current
				g.sync = unknown
			default:
			// how to handle demoted node, it's neither primary nor standby
				g.state = normal
				g.target = current
				g.sync = unknown
		store g
	else:
		// send promoting command
		
`group_state_normal()`:

	// set default command: collect status
	
	if now - primary_time_updated > GAP_PROMOTE:
		if safe_promote && standby is up:
			// prepare promotion
			g.state = promoting
	else if primary is up:
		// primary is active recently
		if primary_syncrep is on:
			// here we have 3 actions:
			// 1. send write query to ensure that the WAL is in sync
			// 2. turn off primary_syncrep to enable write query on primary
			// 3. idle
			todo
			if primary_time_disconnected < primary_time_updated:
				// at least the last update, the replication was disconnected
				// send turn off command
			else if g.sync != 'sync':
				// group is not sync-able, send write query
		else:
			// primary_syncrep is off, try to turn it on
			if primary_time_disconnected == primary_time_updated ||
				standby_time_disconnected == standby_time_updated:
				// send turn on command


g_update_node(g, node, msg):

	assert node in g.nodes
	// todo:
