etcd discovery
==============

Service register and service discovery with [etcdv2](https://github.com/coreos/etcd/releases/tag/v2.3.7).

1. Worker registers itself to etcd with ttl, and periodically refresh the ttl timeout.
2. Master watches etcd for updates of workers, and modifies the route-map accordingly.