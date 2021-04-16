package etcdwrap

import (
    "context"
    clientv3 "go.etcd.io/etcd/client/v3"
    "log"
    "time"
)

type Lease struct {
    ID  clientv3.LeaseID
    TTL time.Duration
}

type WatchHandlerAttr struct {
    CLI *clientv3.Client
}

var client *clientv3.Client

func CreatConnection(config clientv3.Config) (err error) {
    client, err = clientv3.New(config)
    return
}

func CloseConnection() error {
    err := client.Close()
    return err
}

func LeaseGrand(ttl time.Duration) (Lease, error) {
    ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
    res, err := client.Lease.Grant(ctx, int64(ttl.Seconds()))
    cancel()
    return Lease{ID: res.ID, TTL: time.Duration(res.TTL) * time.Second}, err
}

func KeepAlive(ctx context.Context, lease clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
    ch, err := client.KeepAlive(ctx, lease)
    return ch, err
}

func KeyPutWithIgnoreLease(key, value string) error {
    ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
    _, err := client.Put(ctx, key, value, clientv3.WithIgnoreLease())
    cancel()
    return err
}

func KeyPutWithLease(key, value string, lease clientv3.LeaseID) error {
    ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
    _, err := client.Put(ctx, key, value, clientv3.WithLease(lease))
    cancel()
    return err
}


func WatchHandleFunc(ctx context.Context, key string, handler func(WatchHandlerAttr, clientv3.Event)) {

    ch := client.Watch(ctx, key)
    go func() {
        log.Printf("Run watching for key: '%s'", key)
        for resp := range ch {
            for _, ev := range resp.Events {
                handler(WatchHandlerAttr{CLI: client}, *ev)
            }
        }
        log.Printf("End watching for key: '%s'", key)
    }()
}
