package curator_test

import (
	. "github.com/talbright/go-curator"
	"github.com/talbright/go-zookeeper/zk"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"fmt"
	"path"
	"time"
)

var _ = Describe("Client", func() {
	Context("Connect", func() {
		var client *Client
		AfterEach(func() {
			client.Close()
		})
		It("should connect to server", func() {
			client = NewClient()
			settings := &Settings{
				ZkServers:        zkHosts,
				ZkSessionTimeout: zkSessionTimeout,
			}
			chn, err := client.Connect(settings, zk.WithLogger(&NullLogger{}))
			Expect(err).Should(BeNil())
			events := zkCollectEvents(3, chn)
			Expect(events[0]).Should(MatchEvent(zk.EventSession, zk.StateConnecting))
			Expect(events[1]).Should(MatchEvent(zk.EventSession, zk.StateConnected))
			Expect(events[2]).Should(MatchEvent(zk.EventSession, zk.StateHasSession))
		})
		It("should connect to server and wait for session", func() {
			client = NewClient()
			settings := &Settings{
				ZkServers:               []string{"128.0.0.1:2222"},
				ZkSessionTimeout:        zkSessionTimeout,
				ZkWaitForSessionTimeout: 5 * time.Second,
				ZkWaitForSession:        true,
			}
			_, err := client.Connect(settings, zk.WithLogger(&NullLogger{}))
			Expect(err).Should(Equal(ErrConnectionTimedOut))
		})
	})
	Context("CreatePath", func() {
		var client *Client
		BeforeEach(func() {
			client = zkConnect()
		})
		AfterEach(func() {
			client.Close()
		})
		It("should create the new path /a/b/c", func() {
			basePath := fmt.Sprintf("/test/%s", createUUID())
			patha := path.Join(basePath, "a")
			pathb := path.Join(basePath, "a/b")
			pathc := path.Join(basePath, "a/b/c")
			err := client.CreatePath(pathc, []byte("abc"), zk.WorldACLPermAll)
			Expect(err).Should(BeNil())
			Expect(client).Should(HaveZnode(pathc))
			Expect(client).Should(HaveZnodeData(patha, []byte("abc")))
			acl, _, _ := client.GetACL(patha)
			Expect(acl).Should(Equal(zk.WorldACLPermAll))
			Expect(client).Should(HaveZnodeData(pathb, []byte("abc")))
			acl, _, _ = client.GetACL(pathb)
			Expect(acl).Should(Equal(zk.WorldACLPermAll))
			Expect(client).Should(HaveZnodeData(pathc, []byte("abc")))
			acl, _, _ = client.GetACL(pathc)
			Expect(acl).Should(Equal(zk.WorldACLPermAll))
		})
		It("should create the path /a/b/c/d/e on top of /a/b/c", func() {
			basePath := fmt.Sprintf("/test/%s", createUUID())
			pathc := path.Join(basePath, "a/b/c")
			pathe := path.Join(basePath, "a/b/c/d/e")
			err := client.CreatePath(pathc, []byte("abc"), zk.WorldACLPermAll)
			Expect(err).Should(BeNil())
			err = client.CreatePath(pathe, []byte("de"), zk.WorldACLPermAll)
			Expect(err).Should(BeNil())
			Expect(client).Should(HaveZnode(pathe))
			Expect(client).Should(HaveZnodeData(pathe, []byte("de")))
			Expect(client).Should(HaveZnodeData(pathc, []byte("abc")))
		})
		It("should be an error if the path does not start with a /", func() {
			err := client.CreatePath("a/b/c", zk.NoData, zk.WorldACLPermAll)
			Expect(err).Should(Equal(ErrInvalidPath))
		})
	})
})
