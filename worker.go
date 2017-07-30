package etcd

import (
	"encoding/json"
	"time"
	"context"
	"log"

	"github.com/coreos/etcd/client"
)

type Worker struct {
	Name		string
	Address		string
	KeysAPI		client.KeysAPI
	conf		*EtcdConfig
	stopSignal	chan bool
}

// workerInfo is the service register information to etcd
type WorkerInfo struct {
	Name		string	`json:"name"`
	Address		string	`json:"address"`
}

func NewWorker(name, address string, conf *EtcdConfig) *Worker {
	cfg := client.Config{
		Endpoints:               conf.EtcdNodes,
		Transport:               client.DefaultTransport,
		HeaderTimeoutPerRequest: time.Second * time.Duration(conf.EtcdHeaderTimeout),
	}

	etcdClient, err := client.New(cfg)
	if err != nil {
		log.Fatal("Error: cannot connec to etcd:", err)
	}

	w := &Worker{
		Name:		name,
		Address:	address,
		KeysAPI:	client.NewKeysAPI(etcdClient),
		conf:		conf,
		stopSignal:	make(chan bool, 1),
	}
	return w
}

func (w *Worker) Register() {
	api := w.KeysAPI
	info := &WorkerInfo{
		Name:		w.Name,
		Address:	w.Address,
	}

	ticker := time.NewTicker(time.Second * time.Duration(w.conf.EtcdWorkerHeartbeat))

	key := w.conf.EtcdWorkerDir + w.Name
	value, _ := json.Marshal(info)
	setOptions := &client.SetOptions{TTL: time.Second * time.Duration(w.conf.EtcdWorkerTimeout), Refresh: true, PrevExist: client.PrevExist}

	for {
		// should get first, if not exist, set it
		_, err := api.Get(context.Background(), key, &client.GetOptions{Recursive: true})
		if err != nil {
			if client.IsKeyNotFound(err) {
				if _, err := api.Set(context.Background(),  key, string(value), &client.SetOptions{TTL: time.Second * time.Duration(w.conf.EtcdWorkerTimeout)}); err != nil {
					log.Printf("Fail to set worker: name - '%s', address - '%s', error - '%s'", info.Name, info.Address, err.Error())
				} else {
					log.Printf("Success to set worker: name - '%s', address - '%s'", info.Name, info.Address)
				}
			} else {
				log.Print("Fail to connect to etcd.")
			}
		} else {
			// refresh set to true for not notifying the watcher
			if _, err := api.Set(context.Background(), key, "", setOptions); err != nil {
				log.Printf("Fail to refresh timeout for worker: name - '%s', address - '%s', error - '%s'", info.Name, info.Address, err.Error())
			} else {
				log.Printf("Success to refresh timeout for worker: name - '%s', address - '%s'", info.Name, info.Address)
			}
		}
		select {
		case <-w.stopSignal:
			log.Printf("Worker - %s exist.", w.Name)
			return
		case <-ticker.C:
		}
	}
}

// UnRegister delete registered worker from etcd
func (w *Worker) UnRegister() error {
	w.stopSignal <- true
	w.stopSignal = make(chan bool, 1) // just a hack to avoid multi UnRegister deadlock
	key := w.conf.EtcdWorkerDir + w.Name
	_, err := w.KeysAPI.Delete(context.Background(), key, &client.DeleteOptions{Recursive: true})
	if err != nil {
		log.Printf("Fail to deregister worker - '%s' error - %s", key, err.Error())
	} else {
		log.Printf("Success to deregister worker - '%s'.", key)
	}
	return err
}