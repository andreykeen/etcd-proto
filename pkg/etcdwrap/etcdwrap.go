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

func KeyPutWithIgnoreLease(cli *clientv3.Client, key, value string) error {

    ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
    _, err := cli.Put(ctx, key, value, clientv3.WithIgnoreLease())
    cancel()
    return err
}

func KeyPutWithLease(cli *clientv3.Client, key, value string, lease clientv3.LeaseID) error {

    ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
    _, err := cli.Put(ctx, key, value, clientv3.WithLease(lease))
    cancel()
    return err
}

func LeaseGrand(cli *clientv3.Client, ttl time.Duration) (Lease, error) {

    ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
    res, err := cli.Lease.Grant(ctx, int64(ttl.Seconds()))
    cancel()
    return Lease{ID: res.ID, TTL: time.Duration(res.TTL) * time.Second}, err
}

func WatchHandleFunc(ctx context.Context, cli *clientv3.Client, key string, handler func(watchAttr WatchHandlerAttr, respKey, respValue string)) {

    ch := cli.Watch(ctx, key)
    go func() {
        log.Printf("Run watching for key: '%s'", key)
        for resp := range ch {
            for _, ev := range resp.Events {
                log.Printf("%s %q : %q", ev.Type, ev.Kv.Key, ev.Kv.Value)
                handler(WatchHandlerAttr{CLI: cli}, string(ev.Kv.Key), string(ev.Kv.Value))
            }
        }
        log.Printf("End watching for key: '%s'", key)
    }()
}
