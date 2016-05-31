package curator

import (
	"github.com/talbright/go-zookeeper/zk"
)

//EventType describes the type of event that occured
type EventType uint64

//Limited to 64 event types
const (
	AnyEvent EventType = 1 << iota
	ConnectionEvent
	ChildrenWatchLoadedEvent
	ChildrenWatchChangedEvent
	ChildrenWatchStoppedEvent
	LeaderEventElected
	LeaderEventCandidate
	LeaderEventResigned
	MemberEventRegistered
	MemberEventUnregistered
	DiscoveryEventActive
	DiscoveryEventInactive
)

var eventTypeToName = map[EventType]string{
	AnyEvent:                  "AnyEvent",
	ConnectionEvent:           "ConnectionEvent",
	ChildrenWatchLoadedEvent:  "ChildrenWatchLoadedEvent",
	ChildrenWatchChangedEvent: "ChildrenWatchChangedEvent",
	LeaderEventElected:        "LeaderEventElected",
	LeaderEventCandidate:      "LeaderEventCandidate",
	LeaderEventResigned:       "LeaderEventResigned",
	MemberEventRegistered:     "MemberEventRegistered",
	MemberEventUnregistered:   "MemberEventUnregistered",
	DiscoveryEventActive:      "DiscoveryEventActive",
	DiscoveryEventInactive:    "DiscoveryEventInactive",
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
	Data   map[string]interface{}
}

//NewEvent creates a new event for the most common cases
func NewEvent(event EventType, node *Znode, err error) *Event {
	return &Event{
		Type:  event,
		Node:  node,
		Error: err,
		Data:  make(map[string]interface{}),
	}
}

//IsHasSessionEvent is a shortcut for determing if the event is from zk
//and that event state is zk.StateHasSession
func IsHasSessionEvent(event Event) (session bool) {
	if event.Type == ConnectionEvent {
		if event.Source != nil && event.Source.State == zk.StateHasSession {
			session = true
		}
	}
	return
}

//DeepCopy makes a safe copy of the event
func (e Event) DeepCopy() (ecopy *Event) {
	ecopy = &e
	if ecopy.Source != nil {
		sourceCopy := *e.Source
		ecopy.Source = &sourceCopy
	}
	if ecopy.Node != nil {
		ecopy.Node = e.Node.DeepCopy()
	}
	return
}
