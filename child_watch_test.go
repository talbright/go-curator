package curator_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/talbright/go-curator"
	"github.com/talbright/go-zookeeper/zk"

	"fmt"
	"path"
	"time"
)

func AddItemToChildWatch(item string, root string, client *Client) {
	path := path.Join(root, item)
	if err := client.CreatePath(path, ZkNoData, ZkWorldACL); err != nil {
		panic(fmt.Sprintf("unable to create zk path %s", path))
	}
}

func RemoveItemFromChildWatch(item string, root string, client *Client) {
	path := path.Join(root, item)
	if err := client.Delete(path, 0); err != nil {
		panic(fmt.Sprintf("unable to delete zk node %s", path))
	}
}

var _ = Describe("ChildWatch", func() {
	client := zkConnect()
	var watchListPath string
	BeforeEach(func() {
		watchListPath = fmt.Sprintf("/test/%s_watchlist", createUUID())
		if err := client.CreatePath(watchListPath, ZkNoData, ZkWorldACL); err != nil {
			panic(fmt.Sprintf("unable to create zk path %s", watchListPath))
		}
		items := []string{"a", "b", "c", "d"}
		for _, v := range items {
			child := fmt.Sprintf("%s/%s", watchListPath, v)
			if err := client.CreatePath(child, ZkNoData, ZkWorldACL); err != nil {
				panic(fmt.Sprintf("unable to create zk path %s", watchListPath))
			}
		}
	})
	Context("StopWatching", func() {
		It("should stop watching", func() {
			wl := NewChildWatch(client, watchListPath)
			evntChn, err := wl.WatchChildren()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(evntChn).ShouldNot(BeNil())
			var event Event
			Eventually(evntChn).Should(Receive(&event))
			Expect(event.Type).Should(Equal(ChildrenWatchLoadedEvent))
			/*
				When a ChildWatch is stopped the goroutine can't exit until a watch
				event from zookeeper is received (a limitation imposed by go-zookeeper)

			*/
			By("stopping events from being xmitted")
			err = wl.StopWatching()
			Expect(err).ShouldNot(HaveOccurred())
			AddItemToChildWatch("e", watchListPath, client)
			Eventually(evntChn, "3s").ShouldNot(Receive(&event))
			RemoveItemFromChildWatch("a", watchListPath, client)
			Eventually(evntChn, "3s").ShouldNot(Receive(&event))

			By("preventing the watch from being restarted")
			err = wl.StopWatching()
			Expect(err).Should(Equal(ErrWatchStopped))

			By("preventing the watch from being stopped again")
			_, err = wl.WatchChildren()
			Expect(err).Should(Equal(ErrWatchStopped))
		})
	})
	Context("WatchChildren", func() {
		It("should load list", func() {
			wl := NewChildWatch(client, watchListPath)
			evntChn, err := wl.WatchChildren()
			By("finding existing children and xmitting that in an event")
			Expect(err).ShouldNot(HaveOccurred())
			Expect(evntChn).ShouldNot(BeNil())
			var event Event
			Eventually(evntChn).Should(Receive(&event))
			Expect(event.Type).Should(Equal(ChildrenWatchLoadedEvent))
			added := event.Data["added"].(map[string]Znode)
			removed := event.Data["removed"].(map[string]Znode)
			Expect(added).Should(HaveLen(4))
			Expect(removed).Should(HaveLen(0))
			Expect(added).Should(HaveKey("a"))
			Expect(added).Should(HaveKey("b"))
			Expect(added).Should(HaveKey("c"))
			Expect(added).Should(HaveKey("d"))
			By("maintaining the correct map of children")
			items := wl.GetChildren()
			Expect(items).Should(HaveLen(4))
			Expect(items).Should(HaveKey("a"))
			Expect(items).Should(HaveKey("b"))
			Expect(items).Should(HaveKey("c"))
			Expect(items).Should(HaveKey("d"))
		})
		It("should notice list additions", func() {
			wl := NewChildWatch(client, watchListPath)
			evntChn, err := wl.WatchChildren()
			Expect(err).ShouldNot(HaveOccurred())
			var event Event
			Eventually(evntChn).Should(Receive(&event))
			Expect(event.Type).Should(Equal(ChildrenWatchLoadedEvent))
			By("observing the added child")
			AddItemToChildWatch("e", watchListPath, client)
			Eventually(evntChn).Should(Receive(&event))
			Expect(event.Type).Should(Equal(ChildrenWatchChangedEvent))
			added := event.Data["added"].(map[string]Znode)
			removed := event.Data["removed"].(map[string]Znode)
			Expect(added).Should(HaveLen(1))
			Expect(removed).Should(HaveLen(0))
			Expect(added).Should(HaveKey("e"))
			By("updating the children map")
			items := wl.GetChildren()
			Expect(len(items)).Should(Equal(5))
			Expect(items).Should(HaveKey("a"))
			Expect(items).Should(HaveKey("b"))
			Expect(items).Should(HaveKey("c"))
			Expect(items).Should(HaveKey("d"))
			Expect(items).Should(HaveKey("e"))
		})
		It("should notice list removals", func() {
			wl := NewChildWatch(client, watchListPath)
			evntChn, err := wl.WatchChildren()
			Expect(err).ShouldNot(HaveOccurred())
			var event Event
			Eventually(evntChn).Should(Receive(&event))
			Expect(event.Type).Should(Equal(ChildrenWatchLoadedEvent))
			By("observing the removed child")
			RemoveItemFromChildWatch("a", watchListPath, client)
			Eventually(evntChn).Should(Receive(&event))
			Expect(event.Type).Should(Equal(ChildrenWatchChangedEvent))
			added := event.Data["added"].(map[string]Znode)
			removed := event.Data["removed"].(map[string]Znode)
			Expect(added).Should(HaveLen(0))
			Expect(removed).Should(HaveLen(1))
			Expect(removed).Should(HaveKey("a"))
			By("updating the children map")
			items := wl.GetChildren()
			Expect(items).Should(HaveLen(3))
			Expect(items).ShouldNot(HaveKey("a"))
			Expect(items).Should(HaveKey("b"))
			Expect(items).Should(HaveKey("c"))
			Expect(items).Should(HaveKey("d"))
		})
		It("should be an error when list path is invalid", func() {
			wl := NewChildWatch(client, "/test/invalidnode")
			_, err := wl.WatchChildren()
			Expect(err).Should(Equal(ErrInvalidPath))
		})
		It("should be an error event when path to children disappears", func() {
			watchListPath = fmt.Sprintf("/test/%s_watchlist", createUUID())
			if err := client.CreatePath(watchListPath, ZkNoData, ZkWorldACL); err != nil {
				panic(fmt.Sprintf("unable to create zk path %s", watchListPath))
			}
			wl := NewChildWatch(client, watchListPath)
			wl.MaxRetryElapsedTime = 5 * time.Second
			evntChn, _ := wl.WatchChildren()
			var event Event
			Eventually(evntChn).Should(Receive(&event))
			Expect(event.Type).Should(Equal(ChildrenWatchLoadedEvent))
			go func() {
				time.Sleep(5 * time.Second)
				if err := client.Delete(watchListPath, 0); err != nil {
					panic(fmt.Sprintf("unable to delete zk node %s", watchListPath))
				}
			}()
			Eventually(evntChn, "25s", "1s").Should(Receive(&event))
			Expect(event.Type).Should(Equal(ChildrenWatchChangedEvent))
			Expect(event.Error).Should(Equal(zk.ErrNoNode))
		})
	})
})
