package monitor

import "log"

func sanity_check(g *AutoGroup, resMessage *ResponseMessage)  {

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

func debug4(format string, args...interface{}) {
	if _debug_enabled {
		if n := len(format); n > 0 && format[n-1] != '\n' {
			format += "\n"
		}
		log.Printf(format, args...)
	}
}