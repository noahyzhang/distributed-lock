package fair_lock

import (
	"distributed-lock/api"
	"github.com/redis/go-redis/v9"
	"testing"
	"time"
)

func TestLock(t *testing.T) {
	lockName := "redis_fair_lock_test_name"
	redisClient := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379",
		Password: "123",
		DB:       0,
	})
	defer redisClient.Close()
	redisFairLock, err := api.NewFairLockByRedis(lockName, redisClient)
	if err != nil {
		t.Errorf("create fair lock with redis failed, err: %v", err)
		return
	}

	// 加锁 & 解锁
	if err = redisFairLock.Lock(5000); err != nil {
		t.Errorf("lock err: %v\n", err)
		return
	}
	t.Logf("lock success of lockName: %v\n", lockName)

	time.Sleep(200 * time.Millisecond)

	if err = redisFairLock.Unlock(); err != nil {
		t.Errorf("unlock err: %v\n", err)
		return
	}
	t.Logf("unlock success of lockName: %v\n", lockName)
}

func TestTryLock(t *testing.T) {
	lockName := "redis_fair_lock_test_name"
	redisClient := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379",
		Password: "123",
		DB:       0,
	})
	defer redisClient.Close()
	redisFairLock, err := api.NewFairLockByRedis(lockName, redisClient)
	if err != nil {
		t.Errorf("create fair lock with redis failed, err: %v", err)
		return
	}

	// 尝试加锁 & 解锁
	if err = redisFairLock.TryLock(5000, 5000); err != nil {
		t.Errorf("tryLock err: %v\n", err)
		return
	}
	t.Logf("tryLock success of lockName: %v\n", lockName)

	time.Sleep(200 * time.Millisecond)

	if err = redisFairLock.Unlock(); err != nil {
		t.Errorf("unlock err: %v\n", err)
		return
	}
	t.Logf("unlock success of lockName: %v\n", lockName)
}
