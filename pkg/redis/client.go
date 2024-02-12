package redis

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"time"
)

type Client struct {
	client        *redis.Client
	option        *redis.Options
	clientContext context.Context
}

func NewClient(ctx context.Context, opt *redis.Options) *Client {
	client := redis.NewClient(opt)
	return &Client{
		client:        client,
		option:        opt,
		clientContext: ctx,
	}
}

func NewClientWithOption(opt *redis.Options) *Client {
	client := redis.NewClient(opt)
	return &Client{
		client:        client,
		option:        opt,
		clientContext: context.TODO(),
	}
}

func NewClientWithDefaultOption(addr, password string) *Client {
	opt := &redis.Options{
		Addr:     addr,
		Password: password,
		OnConnect: func(ctx context.Context, cn *redis.Conn) error {
			fmt.Println("Connected to Redis!")
			return nil
		},
		DB:           0,
		DialTimeout:  10 * time.Second, // 连接超时时间为 10 秒
		ReadTimeout:  5 * time.Second,  // 读取超时时间为 5 秒
		WriteTimeout: 5 * time.Second,  // 写入超时时间为 5 秒
		PoolSize:     20,               // 连接池最大连接数为 20
		MinIdleConns: 5,                // 连接池最小空闲连接数为 5
		MaxRetries:   2,                // 失败时最大重试次数为 2
	}
	client := redis.NewClient(opt)
	return &Client{
		client:        client,
		option:        opt,
		clientContext: context.TODO(),
	}
}

func (rc *Client) Close() error {
	return rc.client.Close()
}

func (rc *Client) ZAdd(key string, score int, data string) error {
	z := &redis.Z{Score: float64(score), Member: data}
	_, err := rc.client.ZAdd(rc.clientContext, key, z).Result()
	return err
}

func (rc *Client) ZRangeFirst(key string) ([]interface{}, error) {
	// ZRANGE key 0 0 WITHSCORES
	val, err := rc.client.ZRangeWithScores(rc.clientContext, key, 0, 0).Result()
	if err != nil {
		return nil, err
	}
	if len(val) == 1 {
		score := val[0].Score
		member := val[0].Member
		data := []interface{}{score, member}
		return data, nil
	}
	return nil, nil
}

func (rc *Client) ZRem(key string, member string) error {
	_, err := rc.client.ZRem(rc.clientContext, key, member).Result()
	return err
}

func (rc *Client) SetWithTimeout(key string, data string, expiration time.Duration) error {
	_, err := rc.client.Set(rc.clientContext, key, data, expiration).Result()
	return err
}

func (rc *Client) Set(key string, data string) error {
	_, err := rc.client.Set(rc.clientContext, key, data, -1).Result()
	return err
}

func (rc *Client) Get(key string) (string, error) {
	val, err := rc.client.Get(rc.clientContext, key).Result()
	return val, err
}

func (rc *Client) Del(key string) error {
	_, err := rc.client.Del(rc.clientContext, key).Result()
	return err
}
