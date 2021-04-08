package main

import (
	"context"
	"github.com/google/uuid"
	clientv3 "go.etcd.io/etcd/client/v3"
	"log"
	"os"
	"os/signal"
	"path"
	"syscall"
	"time"
)

type TopicLease struct {
	ID  clientv3.LeaseID
	TTL time.Duration
}

var (
	etcdDialTimeout    time.Duration = time.Second * 10
	etcdRequestTimeout time.Duration = time.Second * 10
	keyTTL             time.Duration = time.Second * 300
)

func keyPutWithIgnoreLease(cli *clientv3.Client, key, value string) {

	ctx, cancel := context.WithTimeout(context.Background(), etcdRequestTimeout)
	_, err := cli.Put(ctx, key, value, clientv3.WithIgnoreLease())
	cancel()
	if err != nil {
		log.Fatal(err)
	}
}

func keyPutWithLease(cli *clientv3.Client, key, value string, lease clientv3.LeaseID) {

	ctx, cancel := context.WithTimeout(context.Background(), etcdRequestTimeout)
	_, err := cli.Put(ctx, key, value, clientv3.WithLease(lease))
	cancel()
	if err != nil {
		log.Fatal(err)
	}
}

func leaseGrand(cli *clientv3.Client, ttl time.Duration) TopicLease {

	ctx, cancel := context.WithTimeout(context.Background(), etcdRequestTimeout)
	res, err := cli.Lease.Grant(ctx, int64(ttl.Seconds()))
	cancel()
	if err != nil {
		log.Fatal(err)
	}
	return TopicLease{ID: res.ID, TTL: time.Duration(res.TTL) * time.Second}
}

func createTopic(cli *clientv3.Client, id uuid.UUID, ttl time.Duration) {

	// Grand lease for topic
	topicLease := leaseGrand(cli, ttl)
	log.Printf("Lease %x granted with %.0fs TTL", topicLease.ID, topicLease.TTL.Seconds())

	// Создание key=value
	keyPutWithLease(cli, "/harvesters/"+id.String()+"/state", "connected", topicLease.ID)
	keyPutWithLease(cli, "/harvesters/"+id.String()+"/url", "nil", topicLease.ID)
	keyPutWithLease(cli, "/harvesters/"+id.String()+"/cmd", "nil", topicLease.ID)
}

func doWorker(ch <-chan string) {
	n := 0
	for {
		time.Sleep(time.Millisecond * 100)
		n++
		if n > 50 {
			n = 0
			log.Printf("Idle work")
		}

		select {
		case msg := <-ch:
			log.Printf("doWork end with msg: %s", msg)
			return
		default:
		}
	}
}

func main() {

	// Генерация UUID. Является идентификатором харвестера
	harvUUID := uuid.New()

	log.SetFlags(log.Ldate | log.Ltime | log.Lmsgprefix)
	log.SetPrefix("[" + harvUUID.String() + "] ")
	log.Printf("Run %s\n", path.Base(os.Args[0]))

	// Создание подключения к Etcd
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: etcdDialTimeout,
	})
	if err != nil {
		log.Fatalln("Error connection", err)
	}
	defer func() {
		log.Printf("Close connection with etcd")
		cli.Close()
	}()
	log.Printf("Open connection with etcd")

	createTopic(cli, harvUUID, keyTTL)

	// TODO: Подписаться на key /cmd

	//TODO: Тяжёлая инициализация
	// После которой необходимо изменить значение ключа /harvesters/uuid/state с 'connected' на 'ready'

	chWork := make(chan string)
	go doWorker(chWork)

	// Обработка сигналов из ОС
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigs:
		log.Printf("Catch %s signal. Exit initialization", sig.String())
		chWork <- "exit"
	}

	log.Printf("End %s\n", path.Base(os.Args[0]))
}
