package main

import (
	"context"
	"github.com/google/uuid"
	clientv3 "go.etcd.io/etcd/client/v3"
	"harvester/pkg/etcdwrap"
	"log"
	"os"
	"os/signal"
	"path"
	"syscall"
	"time"
)

var (
	etcdDialTimeout    time.Duration = time.Second * 10
	etcdRequestTimeout time.Duration = time.Second * 10
	keyTTL             time.Duration = time.Second * 30
)

func createTopic(cli *clientv3.Client, id uuid.UUID, ttl time.Duration) etcdwrap.Lease {

	// Grand lease for topic
	topicLease, err := etcdwrap.LeaseGrand(cli, ttl)
	if err != nil {
		log.Fatalf("Can not granted lease: %s", err)
	}
	log.Printf("Lease %x granted with %.0fs TTL", topicLease.ID, topicLease.TTL.Seconds())

	// Создание key=value
	etcdwrap.KeyPutWithLease(cli, "/harvesters/"+id.String()+"/state", "connected", topicLease.ID)
	etcdwrap.KeyPutWithLease(cli, "/harvesters/"+id.String()+"/url", "nil", topicLease.ID)
	etcdwrap.KeyPutWithLease(cli, "/harvesters/"+id.String()+"/cmd", "nil", topicLease.ID)

	return topicLease
}

func doWorker(ch chan string) {
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
			ch <- "done"
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
	defer log.Printf("End %s\n", path.Base(os.Args[0]))

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

	//----- Первичное создание ключей в etcd
	topicLease := createTopic(cli, harvUUID, keyTTL)

	//----- Keep alive lease
	ctxKA, cancelKA := context.WithCancel(context.Background())
	chKA, errKA := cli.KeepAlive(ctxKA, topicLease.ID)
	if errKA != nil {
		log.Fatalf("Can not run Keep Alive: %s", errKA)
	}
	defer func() {
		cancelKA()
		log.Println("Waiting KeepAlive goroutine...")
		<-chKA
	}()

	// TODO: Подписаться на key /cmd
	//chCmd := cli.Watch(context.Background(), "/harvesters/"+harvUUID.String()+"/cmd", clientv3.WithProgressNotify())

	//TODO: Тяжёлая инициализация
	// После которой необходимо изменить значение ключа /harvesters/uuid/state с 'connected' на 'ready'

	chWork := make(chan string)
	go doWorker(chWork)
	defer func() {
		chWork <- "exit"
		log.Println("Waiting doWorker goroutine...")
		<-chWork
	}()

	// Обработка сигналов из ОС
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	sig := <-quit
	log.Printf("Catch %s signal. Exit initialization", sig.String())

}
