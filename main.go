package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
)

const (
	BNODE_NODE = 1
	BNODE_LEAF = 2

	HEADER               = 4
	BTREE_PAGE_SIZE      = 4096
	BTREE_MAX_KEY_SIZE   = 1000
	BTREE_MAX_VALUE_SIZE = 3000
)

type BNode struct {
	data []byte
}

type BTree struct {
	root uint64 //page pointer

	//functions for managing BNode on-disk
	get func(uint64) BNode // dereference a Page pointer to BNode
	new func(BNode) uint64 //allocate a new page
	del func(uint64)       //deallocate a new page
}

func init() {
	node1Max := HEADER + 8 + 2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VALUE_SIZE
	if !(node1Max <= BTREE_PAGE_SIZE) {
		panic("Node Page configuration violation")
	}
}

// Methods to get stuff from our BNode byte array
// Header
func (bnode BNode) getNodeType() uint16 {
	return binary.LittleEndian.Uint16(bnode.data)
}
func (bnode BNode) getNumberOfKeys() uint16 {
	return binary.LittleEndian.Uint16(bnode.data[2:4])
}
func (bnode BNode) setHeaders(nodeType uint16, numberOfKeys uint16) {
	binary.LittleEndian.PutUint16(bnode.data[0:2], nodeType)
	binary.LittleEndian.PutUint16(bnode.data[2:4], numberOfKeys)
}

// Pointer
func (bnode BNode) getPointer(index uint16) uint64 {
	if index >= bnode.getNumberOfKeys() {
		panic("getPointer called with index greater than number of keys for the node")
	}
	pos := HEADER + 8*index
	return binary.LittleEndian.Uint64(bnode.data[pos:])
}

func (bnode BNode) setPointer(index uint16, value uint64) {
	if index >= bnode.getNumberOfKeys() {
		panic("setPointer called with index greater than number of keys for the node")
	}
	pos := HEADER + 8*index
	binary.LittleEndian.PutUint64(bnode.data[pos:], value)
}

// offset list
func offsetPosition(bnode BNode, index uint16) uint16 {
	if index > bnode.getNumberOfKeys() || index < 1 {
		panic("offsetPosition called with index greater than number of keys for the node or index is less than 1")
	}
	return HEADER + 8*bnode.getNumberOfKeys() + (2 * (index - 1))
}

func (bnode BNode) getOffset(index uint16) uint16 {
	if index == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16(bnode.data[offsetPosition(bnode, index):])
}

func (bnode BNode) setOffset(index uint16, offset uint16) {
	binary.LittleEndian.PutUint16(bnode.data[offsetPosition(bnode, index):], offset)
}

// key-value list
func (bnode BNode) getKeyValuePosition(index uint16) uint16 {
	offset := bnode.getOffset(index)
	return HEADER + 8*bnode.getNumberOfKeys() + (2*bnode.getNumberOfKeys() - 1) + offset
}

func (bnode BNode) getKey(index uint16) []byte {
	if index >= bnode.getNumberOfKeys() {
		panic("getKey called with index greater than number of keys for the node")
	}
	kvPos := bnode.getKeyValuePosition(index)
	keylen := binary.LittleEndian.Uint16(bnode.data[kvPos:])
	return bnode.data[kvPos+4:][:keylen]
}

func (bnode BNode) getValue(index uint16) []byte {
	if index >= bnode.getNumberOfKeys() {
		panic("getValue called with index greater than number of keys for the node")
	}
	kvPos := bnode.getKeyValuePosition(index)
	keylen := binary.LittleEndian.Uint16(bnode.data[kvPos:])
	valueLen := binary.LittleEndian.Uint16(bnode.data[kvPos+2:])
	return bnode.data[kvPos+4+keylen:][:valueLen]
}

// node size in bytes
func (bnode BNode) nbytes() uint16 {
	return bnode.getKeyValuePosition(bnode.getNumberOfKeys())
}

// 8 6 4 1
func nodeLookUp(node BNode, key []byte) uint16 {
	nKeys := uint16(node.getNumberOfKeys())
	var i uint16 = 1
	var found uint16 = 0
	for ; i < nKeys; i++ {
		k := node.getKey(i)
		c := bytes.Compare(k, key)
		if c <= 0 {
			found = i
		} else if c > 0 {
			break
		}
	}
	return found
}

func leafInsert(old BNode, new BNode, index uint16, key []byte, value []byte) {
	new.setHeaders(BNODE_LEAF, old.getNumberOfKeys()+1)
	bnodeAppendRange(new, old, 0, 0, index)
	bnodeAppendKV(new, 0, key, value, index)
	bnodeAppendRange(new, old, index+1, index, old.getNumberOfKeys()-index)
}

func leafUpdate(old BNode, new BNode, index uint16, key []byte, value []byte) {
	new.setHeaders(BNODE_LEAF, old.getNumberOfKeys())
	bnodeAppendRange(new, old, 0, 0, index)
	bnodeAppendKV(new, 0, key, value, index)
	bnodeAppendRange(new, old, index+1, index+1, old.getNumberOfKeys()-(index+1))
}

// part of treeInsert(): KV insert to an internal node
func nodeInsert(tree *BTree, new BNode, node BNode, index uint16, key []byte, value []byte) {
	nodePointer := node.getPointer(index)
	child := tree.get(nodePointer)
	tree.del(nodePointer)
	child = treeInsert(tree, child, key, value)
	//todo 4 nov 2023 - go from here
	// split the result
	nsplit, splited := nodeSplit3(child)
	// update the kid links
	nodeReplaceKidN(tree, new, node, index, splited[:nsplit]...)
}

// replace a link with multiple links
func nodeReplaceKidN(
	tree *BTree, new BNode, old BNode, idx uint16,
	kids ...BNode,
) {
	inc := uint16(len(kids))
	new.setHeaders(BNODE_NODE, old.getNumberOfKeys()+inc-1)
	bnodeAppendRange(new, old, 0, 0, idx)
	for i, node := range kids {
		bnodeAppendKV(new, tree.new(node), node.getKey(0), nil, idx+uint16(i))
	}
	bnodeAppendRange(new, old, idx+inc, idx+1, old.getNumberOfKeys()-(idx+1))
}

// split a bigger-than-allowed node into two.
// the second node always fits on a page.
func nodeSplit2(left BNode, right BNode, old BNode) {
	// code omitted...
	nKeys := uint16(old.getNumberOfKeys())
	left.setHeaders(old.getNodeType(), nKeys/2)
	bnodeAppendRange(left, old, 0, 0, nKeys/2)
	bnodeAppendRange(right, old, 0, (nKeys/2)+1, nKeys)
}

// split a node if it's too big. the results are 1~3 nodes.
func nodeSplit3(old BNode) (uint16, [3]BNode) {
	if old.nbytes() <= BTREE_PAGE_SIZE {
		old.data = old.data[:BTREE_PAGE_SIZE]
		return 1, [3]BNode{old}
	}
	left := BNode{make([]byte, 2*BTREE_PAGE_SIZE)} // might be split later
	right := BNode{make([]byte, BTREE_PAGE_SIZE)}
	nodeSplit2(left, right, old)
	if left.nbytes() <= BTREE_PAGE_SIZE {
		left.data = left.data[:BTREE_PAGE_SIZE]
		return 2, [3]BNode{left, right}
	}
	// the left node is still too large
	leftleft := BNode{make([]byte, BTREE_PAGE_SIZE)}
	middle := BNode{make([]byte, BTREE_PAGE_SIZE)}
	nodeSplit2(leftleft, middle, left)
	if leftleft.nbytes() > BTREE_PAGE_SIZE {
		panic("leftleft.nbytes() > BTREE_PAGE_SIZE in nodesplit3")
	}
	return 3, [3]BNode{leftleft, middle, right}
}

// The main function to insert a key
func treeInsert(tree *BTree, node BNode, key []byte, value []byte) BNode {
	// the result node.
	// it's allowed to be bigger than 1 page and will be split if so
	new := BNode{data: make([]byte, 2*BTREE_PAGE_SIZE)}
	// find where to insert the key
	index := nodeLookUp(node, key)
	//act depending on the node type
	switch node.getNodeType() {
	case BNODE_LEAF:
		if bytes.Equal(key, node.getKey(index)) {
			leafUpdate(node, new, index, key, value)
		} else {
			leafInsert(node, new, index+1, key, value)
		}
	case BNODE_NODE:
		nodeInsert(tree, new, node, index, key, value)
	default:
		panic("Bad node type!")
	}

	return new
}

func bnodeAppendRange(new BNode, old BNode, dstNew uint16, srcOld uint16, n uint16) {
	if dstNew+n > n {
		panic("nodeAppendRange dstNew+n is greater than n")
	}
	if srcOld+n > n {
		panic("nodeAppendRange scrOld+n is greater than n")
	}

	if n == 0 {
		return
	}

	//pointers
	for i := uint16(0); i < n; i++ {
		new.setPointer(dstNew+i, old.getPointer(srcOld+i))
	}

	//offsets
	// offsets
	dstBegin := new.getOffset(dstNew)
	srcBegin := old.getOffset(srcOld)
	for i := uint16(1); i <= n; i++ { // NOTE: the range is [1, n]
		offset := dstBegin + old.getOffset(srcOld+i) - srcBegin
		new.setOffset(dstNew+i, offset)
	}

	// KVs
	begin := old.getKeyValuePosition(srcOld)
	end := old.getKeyValuePosition(srcOld + n)
	copy(new.data[new.getKeyValuePosition(dstNew):], old.data[begin:end])
}

func bnodeAppendKV(new BNode, pointer uint64, key []byte, value []byte, index uint16) {
	//set pointer
	new.setPointer(index, pointer)
	//set key and value
	pos := new.getKeyValuePosition(index)
	binary.LittleEndian.PutUint16(new.data[pos:], uint16(len(key)))
	binary.LittleEndian.PutUint16(new.data[pos+2:], uint16(len(value)))
	copy(new.data[pos+4:], key)
	copy(new.data[pos+4+uint16(len(key)):], value)
	// the offset of the next key
	new.setOffset(index+1, new.getOffset(index)+4+uint16((len(key)+len(value))))
}

func main() {
	saveDataAtomic("/home/kevin/db.file", []byte("kevin"))
}

func saveDataAtomic(path string, data []byte) error {
	tempFile := fmt.Sprintf("%s.tmp.%d", path, rand.Int())
	fp, err := os.OpenFile(tempFile, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0064)
	if err != nil {
		return err
	}
	defer fp.Close()

	_, err = fp.Write(data)
	if err != nil {
		os.Remove(tempFile)
		return err
	}
	return os.Rename(tempFile, path)
}
