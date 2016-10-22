package curator

import "github.com/davecgh/go-spew/spew"

type Spewable interface {
	Spew() string
}

func SpewableWrapper(config *spew.ConfigState, a ...interface{}) string {
	var configState *spew.ConfigState
	if config == nil {
		configState = &spew.ConfigState{MaxDepth: 1, Indent: "\t"}
	} else {
		configState = config
	}
	return configState.Sdump(a...)
}
