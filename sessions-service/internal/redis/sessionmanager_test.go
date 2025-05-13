package redis

import (
	"context"
	"fmt"
	"streaming-system/sessions-service/internal/leasing"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAcquireLeaseShouldReturnSuccessWhenUsageIsLowerThanLimit(t *testing.T) {
	redisClient := getRedisClient(t)
	defer redisClient.Close()

	manager := NewSessionsManager(redisClient)

	ctx := context.Background()
	accountID := "account1"
	leaseID := "lease1"
	lease := &leasing.Lease{
		LeaseID:   leaseID,
		AccountID: accountID,
	}

	lease, err := manager.AcquireLease(ctx, leasing.AcquireSpec{
		AccountID:       accountID,
		LeaseID:         leaseID,
		InitialLifetime: LeaseInitialLifetime,
	})

	require.NoError(t, err)
	require.Equal(t, lease.LeaseID, leaseID)
	require.Equal(t, lease.AccountID, accountID)
}

func TestAcquireLeaseShouldReturnErrorWhenUsageIsHigherThanLimit(t *testing.T) {
	redisClient := getRedisClient(t)
	defer redisClient.Close()

	manager := NewSessionsManager(redisClient)

	ctx := context.Background()
	accountID := "account1"

	// Simulate reaching the leases limit
	for i := 0; i < LeasesLimitPerAccount; i++ {
		_, err := manager.AcquireLease(ctx, leasing.AcquireSpec{
			AccountID:       accountID,
			LeaseID:         fmt.Sprintf("lease%d", i),
			InitialLifetime: LeaseInitialLifetime,
		})
		require.NoError(t, err)
	}

	_, err := manager.AcquireLease(ctx, leasing.AcquireSpec{
		AccountID:       accountID,
		LeaseID:         "leaseexceed",
		InitialLifetime: LeaseInitialLifetime,
	})

	require.Error(t, err)
	require.Equal(t, leasing.ErrAccuireLeaseLimit, err)
}

func TestReleaseLease(t *testing.T) {
	redisClient := getRedisClient(t)
	defer redisClient.Close()

	manager := NewSessionsManager(redisClient)

	ctx := context.Background()
	accountID := "account1"

	// Simulate reaching the leases limit
	for i := 0; i < LeasesLimitPerAccount; i++ {
		_, err := manager.AcquireLease(ctx, leasing.AcquireSpec{
			AccountID:       accountID,
			LeaseID:         fmt.Sprintf("lease%d", i),
			InitialLifetime: LeaseInitialLifetime,
		})
		require.NoError(t, err)
	}

	err := manager.ReleaseLease(ctx, "lease0")
	require.NoError(t, err)

	// Check if we can acquire a new lease after releasing one
	_, err = manager.AcquireLease(ctx, leasing.AcquireSpec{
		AccountID:       accountID,
		LeaseID:         "leasenotexceed",
		InitialLifetime: LeaseInitialLifetime,
	})
	require.NoError(t, err)
}

func TestExpireLeases(t *testing.T) {
	redisClient := getRedisClient(t)
	defer redisClient.Close()

	manager := NewSessionsManager(redisClient)
	ctx := context.Background()

	_, err := manager.AcquireLease(ctx, leasing.AcquireSpec{
		AccountID:       "account1",
		LeaseID:         "lease1",
		InitialLifetime: 0,
	})
	require.NoError(t, err)
	_, err = manager.AcquireLease(ctx, leasing.AcquireSpec{
		AccountID:       "account2",
		LeaseID:         "lease2",
		InitialLifetime: LeaseInitialLifetime,
	})
	require.NoError(t, err)

	expireCount, err := manager.ExpireLeases(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, expireCount)
}

func TestRenewLease(t *testing.T) {
	redisClient := getRedisClient(t)
	defer redisClient.Close()

	manager := NewSessionsManager(redisClient)

	ctx := context.Background()
	accountID := "account1"
	leaseID := "lease1"

	// Acquire a lease
	_, err := manager.AcquireLease(ctx, leasing.AcquireSpec{
		AccountID:       accountID,
		LeaseID:         leaseID,
		InitialLifetime: 1 * time.Second,
	})
	require.NoError(t, err)

	// Renew the lease
	err = manager.RenewLease(ctx, leaseID)
	require.NoError(t, err)

	// No leases should be expired
	time.Sleep(1 * time.Second)
	count, err := manager.ExpireLeases(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, count)
}

func getRedisClient(t *testing.T) *redis.Client {
	mr := miniredis.RunT(t)
	return redis.NewClient(&redis.Options{
		Addr:     mr.Addr(),
		Password: "",
		DB:       0,
	})
}
