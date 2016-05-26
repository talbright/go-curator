package curator

import "time"

type Settings struct {
	Servers               []string
	SessionTimeout        time.Duration
	WaitForSessionTimeout time.Duration
	WaitForSession        bool
}
