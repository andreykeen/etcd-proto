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

func createTopic(ctx context.Context, id uuid.UUID, ttl time.Duration) {

	// Grand lease for topic
	topicLease, err := etcdwrap.LeaseGrand(ttl)
	if err != nil {
		log.Fatalf("Can not granted lease: %s", err)
	}
	log.Printf("Lease %x granted with %.0fs TTL", topicLease.ID, topicLease.TTL.Seconds())

    // Run KeepAlive goroutine
    // Требует отмены контекста для остановки goroutine
    _, errKA := etcdwrap.KeepAlive(ctx, topicLease.ID)
    if errKA != nil {
        log.Fatalf("Can not run Keep Alive: %s", errKA)
    }

    // Создание key,value
	err = etcdwrap.KeyPutWithLease("/harvesters/"+id.String()+"/state", "connected", topicLease.ID)
	if err != nil {
        log.Fatalf("Can not put KeyValue: %s", err)
    }
    log.Printf("Create key: %q : %q", "/harvesters/"+id.String()+"/state", "connected")

	err = etcdwrap.KeyPutWithLease("/harvesters/"+id.String()+"/url", "nil", topicLease.ID)
    if err != nil {
        log.Fatalf("Can not put KeyValue: %s", err)
    }
    log.Printf("Create key: %q : %q", "/harvesters/"+id.String()+"/url", "nil")

	err = etcdwrap.KeyPutWithLease("/harvesters/"+id.String()+"/cmd", "nil", topicLease.ID)
    if err != nil {
        log.Fatalf("Can not put KeyValue: %s", err)
    }
    log.Printf("Create key: %q : %q", "/harvesters/"+id.String()+"/cmd", "nil")

    // Run Watcher goroutine
    // Подписка на изменение ключа '/harvesters/UUID/cmd'
    etcdwrap.WatchHandleFunc(ctx, "/harvesters/"+id.String()+"/cmd", handlerCmd)
}


func handlerCmd(watchAttr etcdwrap.WatchHandlerAttr, event clientv3.Event) {
    log.Printf("handlerCmd: %s %q : %q", event.Type, event.Kv.Key, event.Kv.Value)
}

func main() {

	// Генерация UUID. Является идентификатором харвестера
	harvUUID := uuid.New()

	// Конфигурирование логирования
	log.SetFlags(log.Ldate | log.Ltime | log.Lmsgprefix)
	log.SetPrefix("[" + harvUUID.String() + "] ")
	log.Printf("Run %s\n", path.Base(os.Args[0]))
	defer log.Printf("End %s\n", path.Base(os.Args[0]))

	// Создание подключения к Etcd
	err := etcdwrap.CreatConnection(clientv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: etcdDialTimeout,
	})
	if err != nil {
		log.Fatalln("Error connection", err)
	}
	defer func() {
		log.Printf("Close connection with etcd")
		err = etcdwrap.CloseConnection()
        if err != nil {
            log.Printf("Can not correctly closed connection with etcd: %s", err)
        }
    }()
	log.Printf("Open connection with etcd")


	// Основной контекст для передачи в другие goroutine
    ctxMain, cancelMain := context.WithCancel(context.Background())
    defer func() {
        log.Println("Cancelling goroutine...")
        cancelMain()
    }()


	// Создание ключей в etcd, запуск KeepAlive и Watchers
	createTopic(ctxMain, harvUUID, keyTTL)


	// Обработка сигналов из ОС
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	sig := <-quit
	log.Printf("Catch %s signal. Exit initialization", sig.String())
}
