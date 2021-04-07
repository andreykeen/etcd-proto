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

var clientID string = "ee4f3f77-8b54-4e88-a993-5a5aa5217e82"


func createTopic(cli *clientv3.Client, id string) {

    ctx, cancel := context.WithTimeout(context.Background(), time.Second * 5)
    resp, err := cli.Put(ctx, "/harvesters/states/" + id, "connected")
    cancel()
    if err != nil {
        log.Fatal(err)
    }

    fmt.Println(resp)
}


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

    createTopic(cli, clientID)

    //ctx, cancel := context.WithTimeout(context.Background(), time.Second * 5)
    //resp, err := cli.Get(ctx, "/harvesters/harv-001/status")
    //cancel()
    //if err != nil {
    //   log.Fatal(err)
    //}
    //for _, ev := range resp.Kvs {
    //   fmt.Printf("%s - %s\n", ev.Key, ev.Value)
    //}



    fmt.Printf("End app %s\n", path.Base(os.Args[0]))
}
