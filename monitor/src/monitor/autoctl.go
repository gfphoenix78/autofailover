package monitor

import (
	"fmt"
	"log"
	"net"
	"net/http"
)

func (ctl *Manager)RunControlServer() {
	listener, err := net.Listen("tcp", "localhost:8888")
	if err != nil {
		log.Fatalln(err)
	}
	http.Handle("/new_group", http.HandlerFunc(ctl.new_group))
	http.Handle("/del_group", http.HandlerFunc(ctl.del_group))
	http.Handle("/add_group_node", http.HandlerFunc(ctl.add_group_node))
	http.Handle("/del_group_node", http.HandlerFunc(ctl.del_group_node))
	http.Handle("/show_group", http.HandlerFunc(ctl.show_group))
	http.Handle("/show_groups", http.HandlerFunc(ctl.show_groups))
	err = http.Serve(listener, nil)
	if err != nil {
		log.Fatalln(err)
	}
}

func (ctl *Manager)new_group(w http.ResponseWriter, r *http.Request)  {
	var err error
	err = r.ParseForm()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	name := r.Form.Get("name")

	log.Println("new group: ", name)
	w.WriteHeader(http.StatusOK)
	msg := fmt.Sprintf("req => new group: '%s'", name)
	w.Write([]byte(msg))
}
func (ctl *Manager)del_group(w http.ResponseWriter, r *http.Request)  {

}

func (ctl *Manager)add_group_node(w http.ResponseWriter, r *http.Request)  {

}
func (ctl *Manager)del_group_node(w http.ResponseWriter, r *http.Request)  {

}

func (ctl *Manager)show_group(w http.ResponseWriter, r *http.Request)  {

}
func (ctl *Manager)show_groups(w http.ResponseWriter, r *http.Request)  {

}