package testing

import (
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/format"
	. "github.com/talbright/go-curator"
	"github.com/talbright/go-zookeeper/zk"

	"fmt"
)

/*
Custom gomega matcher: zk event is the expected type with the expected state.
*/
type EventMatcher struct {
	Type  zk.EventType
	State zk.State
}

func (matcher *EventMatcher) Match(actual interface{}) (success bool, err error) {
	event := actual.(zk.Event)
	return (event.State == matcher.State && event.Type == matcher.Type), nil
}

func (matcher *EventMatcher) FailureMessage(actual interface{}) (message string) {
	return format.Message(actual.(zk.Event), "to match event", fmt.Sprintf("Type: %d, State: %d", matcher.Type, matcher.State))
}

func (matcher *EventMatcher) NegatedFailureMessage(actual interface{}) (message string) {
	return format.Message(actual.(zk.Event), "not to match event", fmt.Sprintf("Type: %d, State: %d", matcher.Type, matcher.State))
}

func MatchEvent(eventType zk.EventType, eventState zk.State) OmegaMatcher {
	return &EventMatcher{Type: eventType, State: eventState}
}

/*
Custom gomega matcher: validates zk node exists.
*/
type HaveZnodeMatcher struct {
	Path string
}

func (matcher *HaveZnodeMatcher) Match(actual interface{}) (success bool, err error) {
	client := actual.(*Client)
	yes, _, err := client.Exists(matcher.Path)
	return yes == true, nil
}

func (matcher *HaveZnodeMatcher) FailureMessage(actual interface{}) (message string) {
	return format.Message(matcher.Path, "to be a znode")
}

func (matcher *HaveZnodeMatcher) NegatedFailureMessage(actual interface{}) (message string) {
	return format.Message(matcher.Path, "not to be a znode")
}

func HaveZnode(path string) OmegaMatcher {
	return &HaveZnodeMatcher{Path: path}
}

/*
Custom gomega matcher: validates zk node data.
*/
type HaveZnodeDataMatcher struct {
	Path string
	Data []byte
}

func (matcher *HaveZnodeDataMatcher) Match(actual interface{}) (success bool, err error) {
	client := actual.(*Client)
	data, _, err := client.Get(matcher.Path)
	if err != nil {
		return false, err
	}
	s1 := string(data[:])
	s2 := string(matcher.Data)
	return s1 == s2, nil
}

func (matcher *HaveZnodeDataMatcher) FailureMessage(actual interface{}) (message string) {
	return format.Message(matcher.Path, "to have znode data")
}

func (matcher *HaveZnodeDataMatcher) NegatedFailureMessage(actual interface{}) (message string) {
	return format.Message(matcher.Path, "not to have znode data")
}

func HaveZnodeData(path string, data []byte) OmegaMatcher {
	return &HaveZnodeDataMatcher{Path: path, Data: data}
}
