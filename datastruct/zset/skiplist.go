package zset

import (
	"math/bits"
	"math/rand"
)

const maxLevel = 16

type Element struct {
	Member string
	Score  float64
}

type node struct {
	Element
	backward *node
	level    []*Level
}

type Level struct {
	forward *node
	span    int64
}

type skipList struct {
	head   *node
	tail   *node
	length int64
	level  int16
}

func makeNode(level int16, score float64, member string) *node {
	newNode := &node{
		Element:  Element{member, score},
		backward: nil,
		level:    make([]*Level, level),
	}
	for i := range newNode.level {
		newNode.level[i] = new(Level)
	}
	return newNode
}

func makeSkipList() *skipList {
	header := makeNode(maxLevel, 0, "")
	sl := &skipList{
		head:   header,
		tail:   nil,
		length: 0,
		level:  1,
	}
	return sl
}

func randomLevel() int16 {
	total := uint64(1)<<uint64(maxLevel) - 1
	k := rand.Uint64() % total
	return maxLevel - int16(bits.Len64(k+1)) + 1
}

func (skiplist *skipList) insert(member string, score float64) *node {
	// rank[i] 代表 update[i]节点 在第i+1层的排名
	// update[i] 代表 第i+1层 需要更新的 node节点
	rank := make([]int64, maxLevel)
	update := make([]*node, maxLevel)

	node := skiplist.head
	for i := skiplist.level - 1; i >= 0; i-- {
		if i == skiplist.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}
		// 获取 rank切片 和 update切片

		if node.level[i] != nil {
			// 在第i层 找到node 它满足:
			// node节点 的 forward节点的Score > score 或
			// node节点 的 forward节点的Score == score && Member < member （字典序）
			for node.level[i].forward != nil &&
				(node.level[i].forward.Score < score ||
					(node.level[i].forward.Score == score && node.level[i].forward.Member < member)) {
				rank[i] += node.level[i].span
				node = node.level[i].forward
			}
		}
		update[i] = node
	}

	// 生成节点
	level := randomLevel()
	node = makeNode(level, score, member)

	// 判断 skiplist 的 level是否需要更新
	if skiplist.level < level {
		// 若需要更新，则将 skiplist的头节点添加到 update中
		// 注意 这些节点在 第i+1层的span是 整个跳表长度; forward 是 nil
		for i := level - 1; i >= skiplist.level; i-- {
			rank[i] = 0
			update[i] = skiplist.head
			update[i].level[i].forward = nil
			update[i].level[i].span = skiplist.length
		}
		skiplist.level = level
	}

	// 更新 update切片中的节点
	for i := level - 1; i >= 0; i-- {
		if update[i] != nil {
			node.level[i].forward = update[i].level[i].forward
			node.level[i].span = update[i].level[i].span - (rank[0] - rank[i])

			update[i].level[i].forward = node
			update[i].level[i].span = (rank[0] - rank[i]) + 1
		}
	}

	// 若有高于level层数的节点，那么它们也需要被更新
	// 需要更新它们的跨步 span
	for i := level; i < skiplist.level; i++ {
		update[i].level[i].span++
	}

	// 设置node的前向指针 backward
	// 如果插入的节点是第一个
	if update[0] == skiplist.head {
		node.backward = nil
	} else {
		node.backward = update[0]
	}
	// 更新update中 第1层节点 下一个节点 的 前向指针 backward
	// 如果插入的元素是最后一个 那么更新 skiplist 的 tail指针
	if node.level[0].forward == nil {
		skiplist.tail = node
	} else {
		update[0].level[0].forward.backward = node
	}

	skiplist.length++
	return node
}
func (skiplist *skipList) removeNode(node *node, update []*node) {
	for i := int16(0); i < skiplist.level; i++ {
		if update[i].level[i].forward == node {
			update[i].level[i].span += node.level[i].span - 1
			update[i].level[i].forward = node.level[i].forward
		} else {
			update[i].level[i].span--
		}
	}
	// 删除的node是最后一个
	if node.level[0].forward == nil {
		skiplist.tail = node.backward
	} else {
		// 若删除的node是第一个 那么node.backward == nil 无需特殊处理
		node.level[0].forward.backward = node.backward
	}
	// 如果删除的node是第一个 可能会影响 skiplist 的 level
	for skiplist.level > 1 && skiplist.head.level[skiplist.level-1].forward == nil {
		skiplist.level--
	}
	skiplist.length--
}

func (skiplist *skipList) remove(member string, score float64) bool {
	update := make([]*node, maxLevel)
	node := skiplist.head
	for i := skiplist.level - 1; i >= 0; i-- {
		for node.level[i].forward != nil &&
			(node.level[i].forward.Score < score ||
				(node.level[i].forward.Score == score && node.level[i].forward.Member < member)) {
			node = node.level[i].forward
		}
		update[i] = node
	}
	delNode := node.level[0].forward
	if delNode != nil && score == delNode.Score && member == delNode.Member {
		skiplist.removeNode(delNode, update)
		return true
	}
	return false
}

func (skiplist *skipList) getRank(member string, score float64) int64 {
	var rank int64 = 0
	node := skiplist.head
	for i := skiplist.level - 1; i >= 0; i-- {
		for node.level[i].forward != nil &&
			(node.level[i].forward.Score < score ||
				// 这里node.level[i].forward.Member <= member 使用 <= 是为了加上 到 targetNode 的 span 才能真正算出排名
				(node.level[i].forward.Score == score && node.level[i].forward.Member <= member)) {
			rank += node.level[i].span
			node = node.level[i].forward
		}

		if node.Member == member {
			return rank
		}
	}
	return 0
}

func (skiplist *skipList) getByRank(rank int64) *node {
	var i int64 = 0
	node := skiplist.head
	for level := skiplist.level - 1; level >= 0; level-- {
		// 找到对应排名的node， 最差情况就是在第一层找（span为1）
		for node.level[level].forward != nil && (i+node.level[level].span <= rank) {
			i += node.level[level].span
			node = node.level[level].forward
		}
		if i == rank {
			return node
		}
	}
	return nil
}
