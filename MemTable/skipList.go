package MemTable

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
)

import . "NAiSP/Log"

type SkipList struct {
	maxHeight int
	height    int
	size      int
	head      *SkipListNode
}

type SkipListNode struct {
	element    *Log
	next       []*SkipListNode
	nodeHeight int
}

func (s *SkipList) roll() int {
	level := 0
	// possible ret values from rand are 0 and 1
	// we stop when we get a 0
	for ; rand.Int31n(2) == 1; level++ {
		if level >= s.maxHeight {
			if level > s.height {
				s.height = level
			}
			return level
		}
	}
	if level > s.height {
		s.height = level
	}
	return level
}

func (s *SkipList) initNode(element *Log) SkipListNode {
	var newNode SkipListNode
	newNode.element = element
	newNode.nodeHeight = s.roll()
	newNode.next = make([]*SkipListNode, newNode.nodeHeight+1, newNode.nodeHeight+1)

	return newNode
}

func InitHeadNode(maxHeight int) *SkipListNode {
	var head SkipListNode
	head.element = CreateLog(nil, nil)
	head.next = make([]*SkipListNode, maxHeight, maxHeight)
	head.nodeHeight = 0
	return &head
}

func InitSkipList(maxHeight int) *SkipList {
	s := SkipList{maxHeight: maxHeight, height: 0, size: 0, head: InitHeadNode(maxHeight)}
	return &s
}

func (s *SkipList) GetNumOfElements() uint {
	return uint(s.size)
}

func (s *SkipList) Search(key string) *Log {
	currentNode := s.head
	index := 0

	for level := s.height; level >= 0; level-- {
		for currentNode.next[level] != nil && string(currentNode.next[level].element.Key) < key {
			index += 1
			currentNode = currentNode.next[level]
		}

		if currentNode.next[level] != nil && string(currentNode.next[level].element.Key) == key {
			return currentNode.next[level].element
		}
	}

	return nil
}

func (s *SkipList) Insert(newValue *Log) {
	newNode := s.initNode(newValue)

	if newNode.nodeHeight > s.height {
		s.height = newNode.nodeHeight
		s.head.nodeHeight = newNode.nodeHeight
	}

	updateNodes := make([]*SkipListNode, s.height+1)

	currentNode := s.head

	for level := s.height; level >= 0; level-- {
		for currentNode.next[level] != nil && string(currentNode.next[level].element.Key) < string(newValue.Key) {
			currentNode = currentNode.next[level]
		}

		updateNodes[level] = currentNode
	}

	for i := 0; i <= newNode.nodeHeight; i++ {
		newNode.next[i] = updateNodes[i].next[i]

		updateNodes[i].next[i] = &newNode
	}

	s.size++
}

func (s *SkipList) Empty() {
	s.size = 0
	s.head = InitHeadNode(s.maxHeight)
}

func (s *SkipList) PrintSkipList() {
	buf := bufio.NewWriter(os.Stdout)
	defer buf.Flush()

	if s.size == 0 {
		fmt.Fprintf(buf, "Skip List is empty!\n")
		return
	}

	var n *SkipListNode
	for i := s.height; i >= 0; i-- {
		n = s.head
		fmt.Fprintf(buf, "Level %d: ", i)
		for (*n).next[i] != nil {
			n = (*n).next[i]
			fmt.Fprintf(buf, "(%v, ", string(n.element.Key))
			fmt.Fprintf(buf, "%v)", string(n.element.Value))
		}
		fmt.Fprintln(buf)
	}
}

func (s *SkipList) Delete(key string) bool {
	found := s.Search(key)

	if found == nil {
		return false
	}

	(*found).Tombstone = true

	return true
}

func (s *SkipList) GetAllLogs() []*Log {
	var allElements []*Log

	current := s.head

	for i := 0; i < s.size; i++ {
		current = current.next[0]
		allElements = append(allElements, current.element)
	}
	return allElements
}
