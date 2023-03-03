package cluster

import (
	"encoding/json"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"
)

var DiscoveryPrefix = "/discovery"

type peer struct {
	ID  int    `json:"id"`
	URL string `json:"url"`
}

type discoveryMessage struct {
	Peers []peer `json:"peers"`
}

type discoveryHandler struct {
	id    int
	peers *sync.Map
}

func (h *discoveryHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	msg := discoveryMessage{}
	h.peers.Range(func(pid, purl any) bool {
		msg.Peers = append(msg.Peers, peer{
			ID:  pid.(int),
			URL: purl.(string),
		})
		return true
	})

	b, err := json.Marshal(msg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Write(b)
}

func (rn *raftNode) newDiscoveryHandler() http.Handler {
	return &discoveryHandler{
		id:    rn.id,
		peers: rn.peers,
	}
}

func (rn *raftNode) discoverCluster(discovery []string) {
	log.Println("start to discover cluster")
	cluster := make(map[int]string)
	for _, peer := range discovery {
		msg, err := requestDiscovery(peer)
		if err != nil {
			continue
		}
		for _, peer := range msg.Peers {
			cluster[peer.ID] = peer.URL
		}
	}

	log.Printf("found peers %v", cluster)
	for pid, purl := range cluster {
		rn.peers.Store(pid, purl)
	}
}

func requestDiscovery(discovery string) (msg discoveryMessage, err error) {
	URL, err := url.Parse(discovery)
	if err != nil {
		return discoveryMessage{}, err
	}
	URL.Path = DiscoveryPrefix

	cli := http.Client{Timeout: time.Second}
	res, err := cli.Get(URL.String())
	if err != nil {
		return discoveryMessage{}, err
	}
	if err := json.NewDecoder(res.Body).Decode(&msg); err != nil {
		return discoveryMessage{}, err
	}

	return
}
