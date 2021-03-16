package monitor

import "log"

func sanity_check(g *auto_group, srm *ServerResponseMessage)  {

}

func assert(cond bool) {
	if !cond {
		panic("Assert failed")
	}
}
func assert_imply(p, q bool) {
	assert(!p || q)
}

const _debug_enabled = true

func debug(format string, args...interface{}) {
	if _debug_enabled {
		log.Printf(format, args)
	}
}