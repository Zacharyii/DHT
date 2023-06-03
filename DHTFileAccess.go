package main

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"math/big"
	"math/bits"
	mrand "math/rand"
	"sort"
)

const (
	K = 3   // 每个桶的最大容量
)

type Peer struct {
	ID    string
	DHT   *DHT
	kvPairs []KeyValue
}

type KeyValue struct {
	key   []byte
	value []byte
}

type DHT struct {
	buckets []Bucket
}

type Node struct {
	ID     string
	Peers  []*Peer
}

type Bucket struct {
	nodes []*Peer
}

func (dht *DHT) findBucketIndex(nodeId string) int {
	// 假设节点ID为数字字符串
	var xorValue int
	fmt.Sscanf(nodeId, "%d", &xorValue)

	return 159 - bits.LeadingZeros(uint(xorValue))
}

func (dht *DHT) insertNode(peer *Peer) {
	index := dht.findBucketIndex(peer.ID)
	bucket := &dht.buckets[index]

	// 判断K桶记录是否小于K个
	if len(bucket.nodes) < K {
		// 若k桶节点数<K，直接插入尾部
		bucket.nodes = append(bucket.nodes, peer)
	} else {
		// 若k桶节点数>K，选择头部记录项Z替换或删除

		// 首先，找到桶中头部节点
		oldestNode := bucket.nodes[0]
		for i := 0; i < K; i++ {
			if oldestNode.ID > bucket.nodes[i].ID {
				oldestNode = bucket.nodes[i]
			}
		}
		// 计算新节点与头部节点的距离
		var xorNew, xorOld int
		fmt.Sscanf(peer.ID, "%d", &xorNew)
		fmt.Sscanf(oldestNode.ID, "%d", &xorOld)
		distance := xorNew ^ xorOld
		// 如果距离较小，则直接替换头部节点
		if distance < (1 << uint(index)) {
			bucket.nodes[0] = peer
		} else {
			// 否则将头部节点删除，并将新节点插入到桶中
			bucket.nodes = bucket.nodes[1:]
			bucket.nodes = append(bucket.nodes, peer)
		}
	}
}

func (n *Node) FindPeer(peerID string) bool {
	for _, peer := range n.Peers {
		if peer.DHT.findPeer(peerID) { // 调用 FindPeer()方法
			return true
		}
	}
	return false
}

func (dht *DHT) findPeer(peerID string) bool {
	// 在 DHT中的160个桶中查找指定ID的Peer
	for _, bucket := range dht.buckets {
		for _, peer := range bucket.nodes {
			if peer.ID == peerID {
				return true
			}
		}
	}
	return false
}

func NewPeer() *Peer {
	return &Peer{
		DHT: &DHT{
			buckets: makeBucket(),
		},
	}
}

func makeBucket() []Bucket {
	buckets := make([]Bucket, 160)
	for i := range buckets {
		buckets[i] = Bucket{nodes: make([]*Peer, 0)}
	}
	return buckets
}

func (p *Peer) SetValue(key, value []byte) bool {
	// 判断 key 是否是 value 的 hash
	hash := sha256.Sum256(value)
	// 如果不是，返回false
	if !bytes.Equal(key, hash[:]) {
		return false
	}

	// 判断当前 Peer 是否已经保存了这个键值对
	// 如果已经保存，则返回true
	for _, bucket := range p.DHT.buckets {
		for _, peer := range bucket.nodes {
			if peer == p {
				continue
			}
			for _, kv := range peer.kvPairs {
				if bytes.Equal(kv.key, key) {
					return true
				}
			}
		}
	}

	// 否则保存这个键值对，并执⾏第3步
	p.kvPairs = append(p.kvPairs, KeyValue{key, value})

	// 判断Key距离⾃⼰的PeerID的距离
	var xorKey, xorSelf int
	fmt.Sscanf(string(key), "%d", &xorKey)
	fmt.Sscanf(p.ID, "%d", &xorSelf)
	//distance := xorKey ^ xorSelf

	// 算出这个节点对应的桶
	bucketIndex := p.DHT.findBucketIndex(string(key))

	// 从对应的桶中选择两个距离 Key 最近的节点，再递归调用 SetValue 方法，然后返回true
	var nodes []*Peer
	for _, peer := range p.DHT.buckets[bucketIndex].nodes {
		if peer == p {
			continue
		}
		nodes = append(nodes, peer)
	}
	sort.Slice(nodes, func(i, j int) bool {
		var xorI, xorJ int
		fmt.Sscanf(nodes[i].ID, "%d", &xorI)
		fmt.Sscanf(nodes[j].ID, "%d", &xorJ)
		return (xorI^xorKey) < (xorJ^xorKey)
	})
	for i := 0; i < 2 && i < len(nodes); i++ {
		if nodes[i].SetValue(key, value) {
			return true
		}
	}

	return true
}

func (p *Peer) GetValue(key []byte) []byte {
	// 判断当前的Key⾃⼰这个Peer是否已经存储对应的value，
	// 如果⾃⼰这个Peer中有，则返回对应的value；
	for _, kv := range p.kvPairs {
		if bytes.Equal(kv.key, key) {
			return kv.value
		}
	}

	// 如果⾃⼰没有存储当前Key，则对当前的Key执⾏⼀次FindNode操作，
	// 找到距离当前Key最近的2个Peer，
	var xorKey, xorSelf int
	fmt.Sscanf(string(key), "%d", &xorKey)
	fmt.Sscanf(p.ID, "%d", &xorSelf)
	bucketIndex := p.DHT.findBucketIndex(string(key))
	var nodes []*Peer
	for _, peer := range p.DHT.buckets[bucketIndex].nodes {
		if peer == p {
			continue
		}
		nodes = append(nodes, peer)
	}
	sort.Slice(nodes, func(i, j int) bool {
		var xorI, xorJ int
		fmt.Sscanf(nodes[i].ID, "%d", &xorI)
		fmt.Sscanf(nodes[j].ID, "%d", &xorJ)
		return (xorI^xorKey) < (xorJ^xorKey)
	})

	// 然后对这两个Peer执⾏GetValue操作，⼀旦有⼀个节点返回value，
	// 则返回校验成功之后的value，
	for i := 0; i < 2 && i < len(nodes); i++ {
		value := nodes[i].GetValue(key)
		if value != nil {
			// 如果有节点返回了值，则需要验证该值是否是正确的值
			hash := sha256.Sum256(value)
			if bytes.Equal(hash[:], key) {
				return value
			} else {
				// 如果值不正确，则继续查找下一个节点
				continue
			}
		}
	}

	// 如果两个节点都没有返回值，则返回 nil
	return nil
}

func main() {
	// 创建 DHT 和 100 个节点
	dht := &DHT{}
	var nodes [100]Node
	for i := 0; i < 100; i++ {
		nodes[i] = Node{
			ID:    fmt.Sprintf("node-%d", i),
			Peers: make([]*Peer, 100),
		}

		for j := 0; j < 100; j++ {
			// 创建一个 Peer 并将其添加到当前节点的 Peer list 中
			peer := NewPeer()
			nodes[i].Peers[j] = peer

			// 将 Peer 插入对应的桶中
			dht.insertNode(peer)
		}
	}

	// 随机生成 200 个字符串
	var keys [][]byte
	for i := 0; i < 200; i++ {
		b := make([]byte, 16)
		rand.Read(b)
		keys = append(keys, b)
	}

	// 计算每个字符串的哈希值并保存到 values 中
	var values [][]byte
	for _, key := range keys {
		hash := sha256.Sum256(key)
		values = append(values, hash[:])
	}

	// 随机选择一个节点执行 SetValue 操作
	randIndex, _ := rand.Int(rand.Reader, big.NewInt(10000))
	nodeIndex := int(randIndex.Int64() % 100)
	peerIndex := 0
	peer := nodes[nodeIndex].Peers[peerIndex]

	// 保存每个键值对的 Key
	var savedKeys [][]byte

	// 调用 SetValue 方法保存每个键值对
	for i, key := range keys {
		value := values[i]
		if peer.SetValue(key, value) {
			savedKeys = append(savedKeys, key)
		}
	}

	// 打印保存的键值对的数量
	fmt.Printf("Saved %d key-value pairs\n", len(savedKeys))

	// 从200个Key中随机选择100个，然后每个Key再去随机找⼀个节点调⽤GetValue操作。
	randKeys := make([][]byte, 100)
	copy(randKeys, keys)
	mrand.Shuffle(len(randKeys), func(i, j int) {
		randKeys[i], randKeys[j] = randKeys[j], randKeys[i]
	})
	randKeys = randKeys[:100]

	// 对于每个随机选择的 Key，随机找一个节点调用 GetValue 方法
	for _, key := range randKeys {
		// 随机选择一个节点
		randIndex, _ := rand.Int(rand.Reader, big.NewInt(10000))
		nodeIndex := int(randIndex.Int64() % 100)
		peerIndex := 0
		peer := nodes[nodeIndex].Peers[peerIndex]

		// 调用 GetValue 方法
		value := peer.GetValue(key)

		// 打印结果
		if value != nil {
			fmt.Printf("Key %x found, value is %x\n", key, value)
		} else {
			fmt.Printf("Key %x not found\n", key)
		}
	}
}