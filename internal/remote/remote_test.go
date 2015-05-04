package remote_test

import (
	"testing"

	"github.com/syncthing/syncthing/internal/remote"
)

func TestNewProcess(t *testing.T) {
	p := remote.NewProcess()
	p.Set(remote.LogFile("s1/test.log"), remote.Argument("-verbose"))
}
