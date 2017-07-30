package etcd

type EtcdConfig struct {
	EtcdNodes           []string `yaml:"etcd_nodes"`
	EtcdHeaderTimeout   int      `yaml:"etcd_header_timeout"`
	EtcdWorkerTimeout   int      `yaml:"etcd_worker_timeout"`
	EtcdWorkerHeartbeat int      `yaml:"etcd_worker_heartbeat"`
	EtcdWorkerDir       string   `yaml:"etcd_worker_dir"`
}
