package curator_test

import (
	. "github.com/talbright/go-curator"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"fmt"
)

var _ = Describe("Event", func() {
	Context("NewEvent", func() {
		It("should create a new event", func() {
			znode := NewZnode("/a/b/c")
			evnt := NewEvent(AnyEvent, znode, ErrInvalidPath)
			Expect(evnt.Node).Should(Equal(znode))
			Expect(evnt.Error).Should(Equal(ErrInvalidPath))
			Expect(evnt.Type).Should(Equal(AnyEvent))
		})
	})
	Context("String", func() {
		It("should print the event type as a string", func() {
			Expect(fmt.Sprintf("%s", AnyEvent)).Should(Equal("AnyEvent"))
		})
	})
})
