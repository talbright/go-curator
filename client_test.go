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
		It("should connect to server", func() {
			client := NewClient()
			err := client.Connect(zkHosts, zkSessionTimeout)
			Expect(err).Should(BeNil())
			events := zkCollectEvents(3, client.EventChannel)
			Expect(events[0]).Should(MatchEvent(zk.EventSession, zk.StateConnecting))
			Expect(events[1]).Should(MatchEvent(zk.EventSession, zk.StateConnected))
			Expect(events[2]).Should(MatchEvent(zk.EventSession, zk.StateHasSession))
		})
	})
	Context("ConnectWithSettings", func() {
		It("should connect to server with settings", func() {
			settings := &ZkConnectionSettings{
				Servers:           zkHosts,
				SessionTimeout:    zkSessionTimeout,
				ConnectionTimeout: zkConnectionTimeout,
				WaitForSession:    false,
			}
			client := NewClient()
			err := client.ConnectWithSettings(settings)
			Expect(err).Should(BeNil())
			events := zkCollectEvents(3, client.EventChannel)
			Expect(events[0]).Should(MatchEvent(zk.EventSession, zk.StateConnecting))
			Expect(events[1]).Should(MatchEvent(zk.EventSession, zk.StateConnected))
			Expect(events[2]).Should(MatchEvent(zk.EventSession, zk.StateHasSession))
		})
		It("should error out if timeout is exceeded when waiting for session", func() {
			settings := &ZkConnectionSettings{
				Servers:           []string{"128.0.0.1:2222"},
				SessionTimeout:    zkSessionTimeout,
				ConnectionTimeout: time.Second,
				WaitForSession:    true,
			}
			client := NewClient()
			err := client.ConnectWithSettings(settings)
			Expect(err).Should(Equal(ErrConnectionTimedOut))
		})
		It("should return immediately if not waiting for session", func() {
			//expected noise in test output similar to
			//2016/05/14 16:28:00 Failed to connect to 128.0.0.1:2222: dial tcp 128.0.0.1:2222: i/o timeout
			settings := &ZkConnectionSettings{
				Servers:           []string{"128.0.0.1:2222"},
				SessionTimeout:    zkSessionTimeout,
				ConnectionTimeout: zkConnectionTimeout,
				WaitForSession:    false,
			}
			client := NewClient()
			err := client.ConnectWithSettings(settings)
			Expect(err).Should(BeNil())
		})
	})
	Context("CreatePath", func() {
		client := zkConnect()
		It("should create the new path /a/b/c", func() {
			basePath := fmt.Sprintf("/test/%s", createUUID())
			patha := path.Join(basePath, "a")
			pathb := path.Join(basePath, "a/b")
			pathc := path.Join(basePath, "a/b/c")
			err := client.CreatePath(pathc, []byte("abc"), ZkWorldACL)
			Expect(err).Should(BeNil())
			Expect(client).Should(HaveZnode(pathc))
			Expect(client).Should(HaveZnodeData(patha, []byte("abc")))
			acl, _, _ := client.GetACL(patha)
			Expect(acl).Should(Equal(ZkWorldACL))
			Expect(client).Should(HaveZnodeData(pathb, []byte("abc")))
			acl, _, _ = client.GetACL(pathb)
			Expect(acl).Should(Equal(ZkWorldACL))
			Expect(client).Should(HaveZnodeData(pathc, []byte("abc")))
			acl, _, _ = client.GetACL(pathc)
			Expect(acl).Should(Equal(ZkWorldACL))
		})
		It("should create the path /a/b/c/d/e on top of /a/b/c", func() {
			basePath := fmt.Sprintf("/test/%s", createUUID())
			pathc := path.Join(basePath, "a/b/c")
			pathe := path.Join(basePath, "a/b/c/d/e")
			err := client.CreatePath(pathc, []byte("abc"), ZkWorldACL)
			Expect(err).Should(BeNil())
			err = client.CreatePath(pathe, []byte("de"), ZkWorldACL)
			Expect(err).Should(BeNil())
			Expect(client).Should(HaveZnode(pathe))
			Expect(client).Should(HaveZnodeData(pathe, []byte("de")))
			Expect(client).Should(HaveZnodeData(pathc, []byte("abc")))
		})
		It("should be an error if the path does not start with a /", func() {
			err := client.CreatePath("a/b/c", ZkNoData, ZkWorldACL)
			Expect(err).Should(Equal(ErrInvalidPath))
		})
	})
})
