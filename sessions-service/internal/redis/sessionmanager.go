package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"streaming-system/sessions-service/internal/leasing"
	"time"

	"github.com/redis/go-redis/v9"
)

const LeasesLimitPerAccount = 5
const LeaseInitialLifetime = 3 * time.Minute
const LeaseRenewalLifetime = 1 * time.Minute

const (
	LeaseKeyPrefix         = "lease:"         // lease:<leaseId> -> JSON of Lease
	AccountLeasesKeyPrefix = "accountLeases:" // accountLeases:<accountId> -> set of account's leases
	ActiveLeasesKey        = "activeLeases"   // activeLeases -> zset of all existing leases with their expiration times
)

func leaseKey(leaseID string) string {
	return LeaseKeyPrefix + leaseID
}

func accountLeasesKey(accountID string) string {
	return AccountLeasesKeyPrefix + accountID
}

var _ leasing.Manager = (*SessionsManager)(nil)

type SessionsManager struct {
	redisClient *redis.Client
}

func NewSessionsManager(redisClient *redis.Client) *SessionsManager {
	return &SessionsManager{
		redisClient: redisClient,
	}
}

func acquireLeaseScript() string {
	return `
		local lease_key = KEYS[1]
		local account_leases_key = KEYS[2]
		local active_leases_key = KEYS[3]

		local leases_limit = tonumber(ARGV[1])
		local lease_id = ARGV[2]
		local lease_payload = ARGV[3]
		local expiration_time = tonumber(ARGV[4])

		local leases_usage = tonumber(redis.call("SCARD", account_leases_key))
		if leases_usage >= leases_limit then
			-- Account has reached the maximum number of leases
			return redis.error_reply("account has reached the maximum number of leases")
		end


		-- Add the lease to the account's leases set
		redis.call("SADD", account_leases_key, lease_id)

		-- Add the leases to the active leases zset
		redis.call("ZADD", active_leases_key, expiration_time, lease_id)

		-- Set the lease key with the payload and expiration time
		redis.call("SET", lease_key, lease_payload)

		return leases_usage
    `
}

func releaseLeaseScript() string {
	return `
		local lease_key = KEYS[1]
		local account_leases_key = KEYS[2]
		local active_leases_key = KEYS[3]

		local lease_id = ARGV[1]

		-- Remove the lease from the account's leases set
		redis.call("SREM", account_leases_key, lease_id)

		-- Remove the lease from the active leases zset
		redis.call("ZREM", active_leases_key, lease_id)

		-- Delete the lease key
		redis.call("DEL", lease_key)

		return tonumber(redis.call("SCARD", account_leases_key))
    `
}

// AcquireLease increases the lease count for the given accountID.
// If the lease already exists, it returns the existing lease.
// If the account has reached the maximum number of leases, it returns an error.
// It creates a new lease with the given leaseID and save it in the Redis database.
// It also adds the lease to the active leases zset and the account's leases set.
func (m *SessionsManager) AcquireLease(ctx context.Context, spec leasing.AcquireSpec) (*leasing.Lease, error) {
	lease, err := m.getLease(ctx, spec.LeaseID)
	if err != nil {
		return nil, err
	}
	if lease != nil {
		return lease, nil
	}

	lease = &leasing.Lease{
		LeaseID:   spec.LeaseID,
		AccountID: spec.AccountID,
	}
	leasePayload, err := json.Marshal(lease)
	if err != nil {
		return nil, err
	}
	expireAt := time.Now().UTC().Add(spec.InitialLifetime)

	keys := []string{
		leaseKey(spec.LeaseID),
		accountLeasesKey(spec.AccountID),
		ActiveLeasesKey,
	}
	values := []interface{}{
		LeasesLimitPerAccount,
		spec.LeaseID,
		string(leasePayload),
		expireAt.Unix(),
	}

	_, redisErr := redis.NewScript(acquireLeaseScript()).Run(ctx, m.redisClient, keys, values).Result()

	if redisErr != nil {
		return nil, mapRedisLuaScriptError(redisErr)
	}
	return lease, nil
}

// ReleaseLease remove the lease with the given leaseID.
// It removes the lease from the active leases zset and the account's leases set.
// It also deletes the lease key.
// If the lease does not exist, it returns an error.
func (m *SessionsManager) ReleaseLease(ctx context.Context, leaseID string) error {
	lease, err := m.getLease(ctx, leaseID)
	if err != nil {
		return err
	}
	if lease == nil {
		return leasing.ErrLeaseNotExists
	}

	keys := []string{
		leaseKey(leaseID),
		accountLeasesKey(lease.AccountID),
		ActiveLeasesKey,
	}
	values := []interface{}{
		leaseID,
	}
	_, redisErr := redis.NewScript(releaseLeaseScript()).Run(ctx, m.redisClient, keys, values).Result()
	if redisErr != nil {
		return mapRedisLuaScriptError(redisErr)
	}
	return nil
}

// RenewLease prolong the lease with the given leaseID.
// It updates the expiration time of the lease in the active leases zset.
// If the lease does not exist, it returns an error.
func (m *SessionsManager) RenewLease(ctx context.Context, leaseID string) error {
	lease, err := m.getLease(ctx, leaseID)
	if err != nil {
		return err
	}
	if lease == nil {
		return leasing.ErrLeaseNotExists
	}

	currentExpireAt, err := m.redisClient.ZScore(ctx, ActiveLeasesKey, leaseID).Result()
	if err != nil {
		if err == redis.Nil {
			return leasing.ErrLeaseNotExists
		}
	}

	if currentExpireAt < float64(time.Now().UTC().Unix()) {
		return leasing.ErrLeaseNotExists
	}

	expireAt := time.Now().UTC().Add(LeaseRenewalLifetime)
	_, err = m.redisClient.ZAdd(ctx, ActiveLeasesKey, redis.Z{
		Score:  float64(expireAt.Unix()),
		Member: leaseID,
	}).Result()
	return err
}

// ExpireLeases removes all expired leases from the system.
// It finds all leases that have expired and releases them.
// It returns the number of leases that were expired.
// It does not return an error if the leases were expired successfully.
func (m *SessionsManager) ExpireLeases(ctx context.Context) (int, error) {
	expiredLeasesCount := 0
	leaseIDs, err := m.redisClient.ZRangeByScore(ctx, ActiveLeasesKey, &redis.ZRangeBy{
		Min:    strconv.Itoa(0),
		Max:    strconv.FormatInt(time.Now().UTC().Unix(), 10),
		Offset: 0,
		Count:  1000,
	}).Result()

	if err != nil {
		return 0, err
	}

	for _, leaseID := range leaseIDs {
		err = m.ReleaseLease(ctx, leaseID)
		if err == nil {
			expiredLeasesCount++
		}
	}
	return expiredLeasesCount, err
}

func (m *SessionsManager) getLease(ctx context.Context, leaseID string) (*leasing.Lease, error) {
	leaseKey := leaseKey(leaseID)
	leasePayload, err := m.redisClient.Get(ctx, leaseKey).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}
	var lease leasing.Lease
	err = json.Unmarshal([]byte(leasePayload), &lease)
	if err != nil {
		return nil, err
	}
	return &lease, nil
}

func mapRedisLuaScriptError(err error) error {
	if redisErr, ok := err.(redis.Error); ok {
		switch redisErr.Error() {
		case "account has reached the maximum number of leases":
			return leasing.ErrAccuireLeaseLimit
		default:
			return fmt.Errorf("unexpected Redis error: %s", redisErr.Error())
		}
	}
	return err
}
