package curator_test

import (
	. "github.com/talbright/go-curator"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"fmt"
)

type testSpewableInterface struct{}

func (t testSpewableInterface) Spew() string {
	return "yes"
}

var _ = Describe("Spewable", func() {
	Context("Interface", func() {
		It("should return a string", func() {
			var spewable Spewable
			spewable = testSpewableInterface{}
			Expect(spewable.Spew()).Should(Equal("yes"))
		})
	})
	Context("SpewableWrapper", func() {
		It("should wrap the call to spew", func() {
			spewed := SpewableWrapper(nil, "foo")
			fmt.Printf("spewed: %q\n", spewed)
			Expect(SpewableWrapper(nil, "foo")).Should(Equal("(string) (len=3) \"foo\"\n"))
		})
	})
})
