package testing

import (
	. "github.com/onsi/gomega"
	"github.com/satori/go.uuid"
	. "github.com/talbright/go-curator"
	"github.com/talbright/go-zookeeper/zk"

	"fmt"
	"time"
)

//TODO grab via dotenv/env
var zkHosts = []string{"127.0.0.1:2181"}
var zkSessionTimeout = time.Second * 20
var zkConnectionTimeout = time.Second * 20

/*
Collects go-zookeeper events.
*/
func ZkCollectEvents(count int, evntChn <-chan zk.Event) []zk.Event {
	var event zk.Event
	events := make([]zk.Event, 0)
	for i := 0; i < count; i++ {
		EventuallyWithOffset(1, evntChn).Should(Receive(&event))
		events = append(events, event)
	}
	return events
}

/*
Create paths (without children) in zk.
*/
func ZkCreatePaths(client *Client, paths ...string) {
	for _, path := range paths {
		if err := client.CreatePath(path, zk.NoData, zk.WorldACLPermAll); err != nil {
			panic(fmt.Sprintf("unable to create zk path: \"%s\"", path))
		}
	}
}

/*
Delete paths (without children) in zk.
*/
func ZkDeletePaths(client *Client, paths ...string) {
	for _, path := range paths {
		if err := client.Delete(path, -1); err != nil {
			panic(fmt.Sprintf("unable to delete zk path %s", path))
		}
	}
}

/*
Create client connection to zk with timeout.
*/
func ZkConnect() (client *Client) {
	client = NewClient()
	settings := &Settings{
		ZkServers:               zkHosts,
		ZkSessionTimeout:        zkSessionTimeout,
		ZkWaitForSessionTimeout: zkConnectionTimeout,
		ZkWaitForSession:        true,
	}
	if _, err := client.Connect(settings, zk.WithLogger(&NullLogger{})); err != nil {
		panic(fmt.Sprintf("Unable to connect to zk: %s", err))
	}
	return client
}

/*
Generates a unique string suitable for using as a node name.
*/
func ZkUniqueNodeName() string {
	return uuid.NewV4().String()
}
