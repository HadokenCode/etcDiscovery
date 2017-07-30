package etcd

import (
	"encoding/json"
	"time"
	"context"
	"sync"
	"strings"
	"log"

	"github.com/coreos/etcd/client"
)

type Master struct {
	sync.RWMutex	// protect members
	members map[string]*Member
	KeysAPI client.KeysAPI
	conf	*EtcdConfig
	isInitialized	bool
}

// Member is a client machine
type Member struct {
	InGroup bool
	Name	string
	Address	string
}

func NewMaster(conf *EtcdConfig) *Master {
	cfg := client.Config{
		Endpoints:               conf.EtcdNodes,
		Transport:               client.DefaultTransport,
		HeaderTimeoutPerRequest: time.Second * time.Duration(conf.EtcdHeaderTimeout),
	}

	etcdClient, err := client.New(cfg)
	if err != nil {
		log.Fatalf("Error: cannot connec to etcd: %s", err.Error())
	}

	master := &Master{
		members:	make(map[string]*Member),
		KeysAPI:	client.NewKeysAPI(etcdClient),
		conf:		conf,
	}

	return master
}

func (m *Master) AddWorker(info *WorkerInfo) {
	member := &Member{
		InGroup:	true,
		Address:	info.Address,
		Name:		info.Name,
	}
	m.members[member.Name] = member
}

func (m *Master) UpdateWorker(info *WorkerInfo) {
	member := m.members[info.Name]
	member.InGroup = true
}

func NodeToWorkerInfo(node *client.Node) *WorkerInfo {
	info := &WorkerInfo{}
	err := json.Unmarshal([]byte(node.Value), info)
	if err != nil {
		log.Printf("Failed to convert node.Value: %v", err)
	}
	return info
}

func (m *Master) WatchWorkers() {
	api := m.KeysAPI

	// Initialize the members map
	if !m.isInitialized {
		// query addresses from etcd
		resp, err := api.Get(context.Background(), m.conf.EtcdWorkerDir, &client.GetOptions{Recursive: true})
		m.isInitialized = true
		if err == nil {
			if resp != nil && resp.Node != nil {
				m.Lock()
				for _, node := range resp.Node.Nodes {
					info := NodeToWorkerInfo(node)
					log.Printf("Add worker: %v when initilizing", info.Name)
					m.AddWorker(info)
				}
				m.Unlock()
			}
		}
	}

	// Watch for changes
	watcher := api.Watcher(m.conf.EtcdWorkerDir, &client.WatcherOptions{
		Recursive: true,
	})
	for {
		res, err := watcher.Next(context.Background())
		if err != nil {
			log.Printf("Error watch workers: %v", err)
			break
		}

		m.Lock()
		if res.Action == "expire" {
			ss := strings.Split(res.Node.Key, "/")
			name := ss[len(ss)-1]
			log.Printf("Expire worker: %s", name)
			member, ok := m.members[name]
			if ok {
				member.InGroup = false
			}
		} else if res.Action == "set" ||  res.Action == "update" {
			info := NodeToWorkerInfo(res.Node)

			if _, ok := m.members[info.Name]; ok {
				log.Printf("Update worker: %v", info.Name)
				m.UpdateWorker(info)
			} else {
				log.Printf("Add worker: %v", info.Name)
				m.AddWorker(info)
			}
		} else if res.Action == "delete" {
			ss := strings.Split(res.Node.Key, "/")
			name := ss[len(ss)-1]
			log.Printf("Delete worker: %v", name)
			delete(m.members, name)
		}
		m.Unlock()
	}
}

func (m *Master) FindByName(name string) *Member {
	m.RLock()
	defer m.RUnlock()

	member, ok := m.members[name]
	if ok {
		return member
	}
	return  nil
}

// For test
func (m *Master) getMembers() map[string]*Member {
	m.RLock()
	defer m.RUnlock()

	return m.members
}
