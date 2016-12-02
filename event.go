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
	WorkCollectorEventChangeset
	WorkCollectorEventLoaded
	WorkLeaderChangeset
	WorkLeaderActive
	WorkLeaderInactive
)

var eventTypeToName = map[EventType]string{
	AnyEvent:                    "AnyEvent",
	ConnectionEvent:             "ConnectionEvent",
	ChildrenWatchLoadedEvent:    "ChildrenWatchLoadedEvent",
	ChildrenWatchChangedEvent:   "ChildrenWatchChangedEvent",
	LeaderEventElected:          "LeaderEventElected",
	LeaderEventCandidate:        "LeaderEventCandidate",
	LeaderEventResigned:         "LeaderEventResigned",
	MemberEventRegistered:       "MemberEventRegistered",
	MemberEventUnregistered:     "MemberEventUnregistered",
	DiscoveryEventActive:        "DiscoveryEventActive",
	DiscoveryEventInactive:      "DiscoveryEventInactive",
	WorkCollectorEventChangeset: "WorkCollectorEventChangeset",
	WorkCollectorEventLoaded:    "WorkCollectorEventLoaded",
	WorkLeaderChangeset:         "WorkLeaderChangeset",
	WorkLeaderActive:            "WorkLeaderActive",
	WorkLeaderInactive:          "WorkLeaderInactive",
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

//IsValidSessionEvent determines if the event type is a ConnectionEvent
//and there is a valid and associated zk event of type zk.EventSession.
func (e Event) IsValidSessionEvent() (yes bool) {
	if e.Type == ConnectionEvent {
		source := e.Source
		if source != nil && source.Type == zk.EventSession {
			yes = true
		}
	}
	return
}

//IsConnectedEvent is a shortcut for determing if the event is a ConnectionEvent
//and represents an underlying zk.StateHasSession
func (e Event) IsConnectedEvent() (connected bool) {
	if e.IsValidSessionEvent() && e.Source.State == zk.StateHasSession {
		connected = true
	}
	return
}

//IsDisConnectedEvent is a shortcut for determing if the event is a
//ConnectionEvent and represents anything except an underlying
//zk.StateHasSession (which means we are not reliably connected and ready to
//process typical zk API requests)
func (e Event) IsDisconnectedEvent() (connected bool) {
	if e.IsValidSessionEvent() && e.Source.State != zk.StateHasSession {
		connected = false
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
