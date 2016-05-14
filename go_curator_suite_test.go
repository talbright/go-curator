package curator_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/format"
	"github.com/satori/go.uuid"
	. "github.com/talbright/go-curator"
	"github.com/talbright/go-zookeeper/zk"

	"fmt"
	"testing"
	"time"
)

//TODO dotenv
var zkHosts = []string{"127.0.0.1:2181"}
var zkSessionTimeout = time.Second * 20
var zkConnectionTimeout = time.Second * 20

func TestGoCurator(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "GoCurator Suite")
}

/*
Create UUID for testing nodes in zk.
*/
func createUUID() string {
	return uuid.NewV4().String()
}

/*
Collects go-zookeeper events.
*/
func zkCollectEvents(count int, evntChn <-chan zk.Event) []zk.Event {
	var event zk.Event
	events := make([]zk.Event, 0)
	for i := 0; i < count; i++ {
		Eventually(evntChn).Should(Receive(&event))
		events = append(events, event)
	}
	return events
}

/*
Create paths (without children) in zk.
*/
func zkCreatePaths(client *Client, paths ...string) {
	for _, path := range paths {
		if err := client.CreatePath(path, ZkNoData, ZkWorldACL); err != nil {
			panic(fmt.Sprintf("unable to create zk path: \"%s\"", path))
		}
	}
}

/*
Delete paths (without children) in zk.
*/
func zkDeletePaths(client *Client, paths ...string) {
	for _, path := range paths {
		if err := client.Delete(path, -1); err != nil {
			panic(fmt.Sprintf("unable to delete zk path %s", path))
		}
	}
}

/*
Create client connection to zk with timeout.
*/
func zkConnect() (client *Client) {
	client = NewClient()
	if err := client.ConnectWithSettings(&ZkConnectionSettings{
		Servers:           zkHosts,
		SessionTimeout:    zkSessionTimeout,
		ConnectionTimeout: zkConnectionTimeout,
		WaitForSession:    true,
	}); err != nil {
		panic(fmt.Sprintf("Unable to connect to zk: %s", err))
	}
	return client
}

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
