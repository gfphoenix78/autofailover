package main

import (
	"log"
	"monitor"
)

func main()  {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	monitor.Main2()
}