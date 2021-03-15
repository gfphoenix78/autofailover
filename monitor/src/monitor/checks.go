package monitor

func sanity_check(g *auto_group, srm *ServerResponseMessage)  {

}

func assert(cond bool) {
	if !cond {
		panic("Assert failed")
	}
}
