package curator

import (
	"github.com/talbright/go-zookeeper/zk"
)

//EventType describes the type of event that occured
type EventType uint64

//Limited to 64 event types
const (
	AnyEvent EventType = 1 << iota
)

var eventTypeToName = map[EventType]string{
	AnyEvent: "AnyEvent",
}

func (e EventType) String() (name string) {
	if val, ok := eventTypeToName[e]; ok {
		name = val
	}
	return
}

//Event represents an event that occurs within the components in curator
type Event struct {
	Type   EventType
	Node   *Znode
	Source *zk.Event
	Error  error
}

//NewEvent creates a new event for the most common cases
func NewEvent(event EventType, node *Znode, err error) *Event {
	return &Event{
		Type:  event,
		Node:  node,
		Error: err,
	}
}
