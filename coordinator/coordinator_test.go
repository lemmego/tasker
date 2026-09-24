package coordinator

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lemmego/tasker"
)

type failedRegisterDriver struct{ tasker.Driver }

func (failedRegisterDriver) RegisterNode(context.Context, tasker.NodeInfo, time.Duration) error {
	return errors.New("register failed")
}

func TestStartRollsBackStateWhenRegistrationFails(t *testing.T) {
	mgr := tasker.NewManager(tasker.WithDriver(failedRegisterDriver{}))
	c := New(mgr, DefaultConfig())

	if err := c.Start(context.Background()); err == nil {
		t.Fatal("Start returned nil")
	}
	if c.running {
		t.Fatal("coordinator remains running after failed start")
	}
	if c.cancel != nil {
		t.Fatal("coordinator retained a cancellation function after failed start")
	}
}
