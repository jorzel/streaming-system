package leasing

import (
	"context"
	"fmt"
)

var ErrAccuireLeaseLimit = fmt.Errorf("account has reached the maximum number of leases")
var ErrLeaseNotExists = fmt.Errorf("lease not exists")

type AcquireSpec struct {
	LeaseID   string
	AccountID string
}

type Lease struct {
	LeaseID   string
	AccountID string
}

type Manager interface {
	AcquireLease(context.Context, AcquireSpec) (*Lease, error)
	ReleaseLease(context.Context, string) error
	RenewLease(context.Context, string) error
	ExpireLeases(context.Context) (int, error)
}
