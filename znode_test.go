package curator_test

import (
	. "github.com/talbright/go-curator"
	"github.com/talbright/go-zookeeper/zk"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Znode", func() {
	var node *Znode
	BeforeEach(func() {
		node = NewZnode("/usr/local/var/base")
	})
	Context("NewZnode", func() {
		It("creates a node using the basename as the name", func() {
			Expect(node.Name).Should(Equal("base"))
			Expect(node.Path).Should(Equal("/usr/local/var/base"))
		})
	})
	Context("Basename", func() {
		It("should extract the basename of the node", func() {
			Expect(node.Basename()).Should(Equal("base"))
		})
	})
	Context("Parent", func() {
		It("should extract the parent name of the node", func() {
			Expect(node.Parent()).Should(Equal("/usr/local/var"))
		})
	})
	Context("DeepCopy", func() {
		It("should create a deep copy of the znode with nil data and stat", func() {
			znodeCopy := node.DeepCopy()
			Expect(znodeCopy.Data).Should(BeNil())
			Expect(znodeCopy.Stat).Should(BeNil())
		})
		It("should create a deep copy of the znode with data and stat", func() {
			node.Stat = &zk.Stat{Version: 123}
			node.Data = []byte("abc")
			znodeCopy := node.DeepCopy()
			Expect(znodeCopy.Stat == node.Stat).ShouldNot(BeTrue())
			node.Stat.Version = 99
			Expect(znodeCopy.Stat.Version).ShouldNot(Equal(node.Stat.Version))
			node.Data[0] = 'z'
			Expect(string(znodeCopy.Data[:])).ShouldNot(Equal(string(node.Data[:])))
		})
	})
})
