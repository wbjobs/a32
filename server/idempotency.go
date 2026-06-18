package server

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisClient interface {
	SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd
	Exists(ctx context.Context, keys ...string) *redis.IntCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd
	Ping(ctx context.Context) *redis.StatusCmd
	Close() error
}

type IdempotencyStore struct {
	client RedisClient
	ttl    time.Duration
}

func NewIdempotencyStore(redisAddr, redisPassword string, redisDB int, ttl time.Duration) *IdempotencyStore {
	client := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: redisPassword,
		DB:       redisDB,
	})
	return &IdempotencyStore{
		client: client,
		ttl:    ttl,
	}
}

func NewIdempotencyStoreWithClient(client RedisClient, ttl time.Duration) *IdempotencyStore {
	return &IdempotencyStore{
		client: client,
		ttl:    ttl,
	}
}

func (s *IdempotencyStore) key(uuid string) string {
	return "slowquery:idempotency:" + uuid
}

func (s *IdempotencyStore) CheckAndSet(ctx context.Context, uuid string) (bool, error) {
	if uuid == "" {
		return true, nil
	}
	key := s.key(uuid)
	ok, err := s.client.SetNX(ctx, key, 1, s.ttl).Result()
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	return true, nil
}

func (s *IdempotencyStore) Exists(ctx context.Context, uuid string) (bool, error) {
	if uuid == "" {
		return false, nil
	}
	key := s.key(uuid)
	n, err := s.client.Exists(ctx, key).Result()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

func (s *IdempotencyStore) Clear(ctx context.Context, uuid string) error {
	if uuid == "" {
		return nil
	}
	key := s.key(uuid)
	return s.client.Del(ctx, key).Err()
}

func (s *IdempotencyStore) Ping(ctx context.Context) error {
	return s.client.Ping(ctx).Err()
}

func (s *IdempotencyStore) Close() error {
	return s.client.Close()
}
