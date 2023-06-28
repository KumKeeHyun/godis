# Godis

## Support Commands

- [set](https://redis.io/commands/set/)
- [get](https://redis.io/commands/get/)
- [mget](https://redis.io/commands/mget/)

## Standalone
```
# ./godis server --host 0.0.0.0 --port 6379
```

## Cluster

### Init Cluster
```
// node1
# ./godis cluster --id 1 \
--listen-client http://0.0.0.0:6379 \
--listen-peer http://127.0.0.1:6300 \
--initial-cluster 1@http://127.0.0.1:6300,2@http://127.0.0.1:16300 \
--waldir ./node-1/wal --snapdir ./node-1/snap

// node2
# ./godis cluster --id 2 \
--listen-client http://0.0.0.0:6380 \
--listen-peer http://127.0.0.1:16300 \
--initial-cluster 1@http://127.0.0.1:6300,2@http://127.0.0.1:16300 \
--waldir ./node-2/wal --snapdir ./node-2/snap


```

![godis-2](https://user-images.githubusercontent.com/44857109/223992249-dee2589b-f80e-453c-83f0-721616634933.gif)

### Join New Node

```
// client
# cluster meet 3 http://127.0.0.1:26300

// node3
# ./godis cluster --id 3 \
--listen-client http://0.0.0.0:6381 \
--listen-peer http://127.0.0.1:26300 \
--initial-cluster 1@http://127.0.0.1:6300,2@http://127.0.0.1:16300,3@http://127.0.0.1:26300 \
--waldir ./node-3/wal --snapdir ./node-3/snap --join
```

```
// client
# cluster meet 4 http://127.0.0.1:36300

// node4
# ./godis cluster --id 4 \
--listen-client http://0.0.0.0:6382 \
--listen-peer http://127.0.0.1:36300 \
--initial-cluster 1@http://127.0.0.1:6300,2@http://127.0.0.1:16300,3@http://127.0.0.1:26300,4@http://127.0.0.1:36300 \
--waldir ./node-4/wal --snapdir ./node-4/snap --join
```

![godis-3](https://user-images.githubusercontent.com/44857109/224008053-b7d7bee6-488e-4886-ac15-45a245019b10.gif)

### Restart Node

```
./godis cluster --id 1 \
--listen-client http://0.0.0.0:6379 \
--listen-peer http://127.0.0.1:6300 \
--initial-cluster 1@http://127.0.0.1:6300,2@http://127.0.0.1:16300,3@http://127.0.0.1:26300,4@http://127.0.0.1:36300 \
--waldir ./node-1/wal --snapdir ./node-1/snap
```