# Godis

## Support Commands

- string
  - [set](https://redis.io/commands/set/)
  - [get](https://redis.io/commands/get/)
  - [mget](https://redis.io/commands/mget/)
- set
  - [sadd](https://redis.io/commands/sadd/)
  - [scard](https://redis.io/commands/scard/)
  - [smembers](https://redis.io/commands/smembers/)
  - [srem](https://redis.io/commands/srem/)

## Standalone


```shell
# Apple M2
$ redis-benchmark -t set -n 200000 -r 100000 -d 10000 -q
SET: 138696.25 requests per second, p50=0.183 msec
```

```shell
$ ./godis server --host 0.0.0.0 --port 6379
```

## Cluster

```shell
# Apple M2
# response after raft log commit mode
$ redis-benchmark -t set -n 20000 -r 100000 -d 1000 -q                                                                       INT ✘  22:20:44
SET: 1723.69 requests per second, p50=29.663 msec
```

### Init Cluster - node 1, 2, 3

```shell
# node1
$ ./godis cluster --id 1 \
--listen-client http://0.0.0.0:6379 \
--listen-peer http://127.0.0.1:6300 \
--initial-cluster 1@http://127.0.0.1:6300,2@http://127.0.0.1:16300,3@http://127.0.0.1:26300 \
--waldir ./node-1/wal --snapdir ./node-1/snap

# node2
$ ./godis cluster --id 2 \
--listen-client http://0.0.0.0:6380 \
--listen-peer http://127.0.0.1:16300 \
--initial-cluster 1@http://127.0.0.1:6300,2@http://127.0.0.1:16300,3@http://127.0.0.1:26300 \
--waldir ./node-2/wal --snapdir ./node-2/snap

# node3
$ ./godis cluster --id 3 \
--listen-client http://0.0.0.0:6381 \
--listen-peer http://127.0.0.1:26300 \
--initial-cluster 1@http://127.0.0.1:6300,2@http://127.0.0.1:16300,3@http://127.0.0.1:26300 \
--waldir ./node-3/wal --snapdir ./node-3/snap

```

![godis-2](https://user-images.githubusercontent.com/44857109/223992249-dee2589b-f80e-453c-83f0-721616634933.gif)

### Join New Node 4

```shell
# client
$ ./godis client
> cluster meet 4 http://127.0.0.1:36300

# node4
$ ./godis cluster --id 4 \
--listen-client http://0.0.0.0:6382 \
--listen-peer http://127.0.0.1:36300 \
--initial-cluster 1@http://127.0.0.1:6300,2@http://127.0.0.1:16300,3@http://127.0.0.1:26300,4@http://127.0.0.1:36300 \
--waldir ./node-4/wal --snapdir ./node-4/snap --join
```

![godis-3](https://user-images.githubusercontent.com/44857109/224008053-b7d7bee6-488e-4886-ac15-45a245019b10.gif)

### Restart Node

```shell
# restart node 1
$ ./godis cluster --id 1 \
--listen-client http://0.0.0.0:6379 \
--listen-peer http://127.0.0.1:6300 \
--initial-cluster 1@http://127.0.0.1:6300,2@http://127.0.0.1:16300,3@http://127.0.0.1:26300,4@http://127.0.0.1:36300 \
--waldir ./node-1/wal --snapdir ./node-1/snap
```

## Kubernetes Controller

- create crd && deploy godis controller

```shell
$ kubectl apply -f ./install/kubernetes/controller/godis-controller.yml
```

- create godis cluster resource

```yml
apiVersion: kumkeehyun.github.com/v1
kind: GodisCluster
metadata:
  name: example-godis
spec:
  name: example-godis
  replicas: 3
```

```shell
$ kubectl get godiscluster
NAME            SPECREPLICAS   STATUSREPLICAS   STATUS    AGE
example-godis   3              3                Running   4s

$ kubectl get godis
NAME              SPECPREFERRED   STATUSSTATE   AGE
example-godis-1   true                          24s
example-godis-2   true                          24s
example-godis-3   true                          24s

$ kubectl get pod -l "cluster-name=example-godis"
NAME                                READY   STATUS    RESTARTS   AGE
example-godis-1-6k5wx               1/1     Running   0          41s
example-godis-2-rphmj               1/1     Running   0          40s
example-godis-3-75g8h               1/1     Running   0          40s

$ kubectl logs example-godis-1-6k5wx
2023/07/08 06:12:16 start server in id(1) listenClient(http://0.0.0.0:6379) listenPeer(http://0.0.0.0:6300) initialCluster([1@http://example-godis-1-endpoint.default.svc.cluster.local:6300,2@http://example-godis-2-endpoint.default.svc.cluster.local:6300,3@http://example-godis-3-endpoint.default.svc.cluster.local:6300]) join(false)
raft2023/07/08 06:12:16 INFO: 1 switched to configuration voters=()
raft2023/07/08 06:12:16 INFO: 1 became follower at term 0
raft2023/07/08 06:12:16 INFO: newRaft 1 [peers: [], term: 0, commit: 0, applied: 0, lastindex: 0, lastterm: 0]
raft2023/07/08 06:12:16 INFO: 1 became follower at term 1
raft2023/07/08 06:12:16 INFO: 1 switched to configuration voters=(2)
raft2023/07/08 06:12:16 INFO: 1 switched to configuration voters=(2 3)
raft2023/07/08 06:12:16 INFO: 1 switched to configuration voters=(1 2 3)
raft2023/07/08 06:12:16 INFO: 1 switched to configuration voters=(1 2 3)
raft2023/07/08 06:12:16 INFO: 1 switched to configuration voters=(1 2 3)
raft2023/07/08 06:12:16 INFO: 1 switched to configuration voters=(1 2 3)
raft2023/07/08 06:12:17 INFO: 1 [term: 1] received a MsgVote message with higher term from 2 [term: 4]
raft2023/07/08 06:12:17 INFO: 1 became follower at term 4
raft2023/07/08 06:12:17 INFO: 1 [logterm: 1, index: 3, vote: 0] cast MsgVote for 2 [logterm: 1, index: 3] at term 4
raft2023/07/08 06:12:17 INFO: raft.node: 1 elected leader 2 at term 4
```

- edit to scaling out

```shell
$ kubectl edit godiscluster example-godis
#(edit spec.replicas 3 -> 4)

$ kubectl get godiscluster
NAME            SPECREPLICAS   STATUSREPLICAS   STATUS    AGE
example-godis   4              4                Running   4m52s

$ kubectl get godis
NAME              SPECPREFERRED   STATUSSTATE   AGE
example-godis-1   true                          4m57s
example-godis-2   true                          4m57s
example-godis-3   true                          4m57s
example-godis-4   false                         14s

$ kubectl get pod -l "cluster-name=example-godis"
NAME                    READY   STATUS    RESTARTS   AGE
example-godis-1-6k5wx   1/1     Running   0          5m9s
example-godis-2-rphmj   1/1     Running   0          5m8s
example-godis-3-75g8h   1/1     Running   0          5m8s
example-godis-4-8d5n9   1/1     Running   0          26s
```

- delete godis-3 (auto healing)

```shell
$ kubectl delete godis exmaple-godis-3

$ kubectl get godis
NAME              SPECPREFERRED   STATUSSTATE   AGE
example-godis-1   true                          6m59s
example-godis-2   true                          6m59s
example-godis-4   false                         2m16s
example-godis-5   false                         4s

$ kubectl get pod -l "cluster-name=example-godis"
NAME                    READY   STATUS    RESTARTS   AGE
example-godis-1-6k5wx   1/1     Running   0          7m11s
example-godis-2-rphmj   1/1     Running   0          7m10s
example-godis-4-8d5n9   1/1     Running   0          2m28s
example-godis-5-k5vhg   1/1     Running   0          16s
```

- edit to scaling in

```shell
$ kubectl edit godiscluster example-godis
#(edit spec.replicas 4 -> 3)

$ kubectl get godiscluster
NAME            SPECREPLICAS   STATUSREPLICAS   STATUS    AGE
example-godis   3              3                Running   9m16s

$ kubectl get godis
NAME              SPECPREFERRED   STATUSSTATE   AGE
example-godis-1   true                          9m13s
example-godis-2   true                          9m13s
example-godis-4   false                         4m30s

$ kubectl get pod -l "cluster-name=example-godis"
NAME                    READY   STATUS    RESTARTS   AGE
example-godis-1-6k5wx   1/1     Running   0          8m42s
example-godis-2-rphmj   1/1     Running   0          8m41s
example-godis-4-8d5n9   1/1     Running   0          3m59s
```