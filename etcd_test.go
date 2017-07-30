package etcd

import (
	"time"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	name1		string	= "client1"
	name2		string	= "client2"

	address1	string	= "10.18.110.1:8080"
	address2	string	= "10.18.110.2:8080"

	members		map[string]*Member = map[string]*Member{
		"client1": &Member{
			InGroup: 	true,
			Name:		"client1",
			Address:	"10.18.110.1:8080",
		},
		"client2": &Member{
			InGroup: 	true,
			Name:		"client2",
			Address:	"10.18.110.2:8080",
		},
	}

	members1	map[string]*Member = map[string]*Member{
		"client1": &Member{
			InGroup: 	true,
			Name:		"client1",
			Address:	"10.18.110.1:8080",
		},
	}

	conf		*EtcdConfig	= &EtcdConfig{
		EtcdNodes:		[]string{"http://127.0.0.1:2379"},
		EtcdHeaderTimeout:	3,
		EtcdWorkerTimeout:	100,
		EtcdWorkerHeartbeat:	30,
		EtcdWorkerDir:		"/etcDiscovery/",
	}
)

var (
	work1	*Worker
	work2	*Worker
	master	*Master
)

func Test_Etcd(t *testing.T) {
	master = NewMaster(conf)
	assert.NotNil(t, master)
	go master.WatchWorkers()

	time.Sleep(time.Second * 1)

	work1 = NewWorker(name1, address1, conf)
	assert.NotNil(t, work1)
	go work1.Register()

	time.Sleep(time.Second * 1)

	work2 = NewWorker(name2, address2, conf)
	assert.NotNil(t, work2)
	go work2.Register()

	time.Sleep(time.Second * 1)
	assert.Equal(t, master.getMembers(), members)

	err := work2.UnRegister()
	assert.Nil(t, err)

	time.Sleep(time.Second * 1)
	assert.Equal(t, master.getMembers(), members1)
}