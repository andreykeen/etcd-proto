package main

import (
    "context"
    "fmt"
    clientv3 "go.etcd.io/etcd/client/v3"
    "log"
    "os"
    "path"
    "time"
)

func main() {

    fmt.Printf("Run app %s\n", path.Base(os.Args[0]))

    cli, err := clientv3.New(clientv3.Config{
        Endpoints: []string{"127.0.0.1:2379"},
        DialTimeout: 5 * time.Second,
    })
    if err != nil {
        log.Fatalln("Error connection", err)
    }
    defer cli.Close()

    ctx, cancel := context.WithTimeout(context.Background(), time.Second * 5)
    resp, err := cli.Get(ctx, "/harvesters/harv-001/status")
    cancel()
    if err != nil {
        log.Fatal(err)
    }
    for _, ev := range resp.Kvs {
        fmt.Printf("%s - %s\n", ev.Key, ev.Value)
    }



    fmt.Printf("End app %s\n", path.Base(os.Args[0]))
}
