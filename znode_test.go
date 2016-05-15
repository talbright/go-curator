package curator_test

import (
	. "github.com/talbright/go-curator"

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
})
