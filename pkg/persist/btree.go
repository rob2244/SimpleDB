package persist

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

type NodeType uint8

type ArrayWriter []byte

// TODO this is stupid find a better wayt to do this
// Or just read and write the byte yourself
func (a ArrayWriter) Write(p []byte) (n int, err error) {
	n = copy(a, p)
	err = nil

	return
}

// Node types
const (
	internalNode NodeType = iota
	leafNode
)

// Common node header layout
const (
	nodeTypeSize         uint32 = 1
	nodeTypeOffset       uint32 = 0
	isRootSize           uint32 = 1
	isRootOffset         uint32 = nodeTypeSize
	parentPointerSize    uint32 = 4
	parentPointerOffset  uint32 = isRootOffset + isRootSize
	commonNodeHeaderSize uint32 = nodeTypeSize + isRootSize + parentPointerSize
)

// Leaf Node Header Layout
const (
	leafNodeNumCellsSize   uint32 = 4
	leafNodeNumCellsOffset uint32 = commonNodeHeaderSize
	leafNodeNextLeafSize   uint32 = 4
	leafNodeNextLeafOffset uint32 = leafNodeNumCellsOffset + leafNodeNumCellsSize
	leafNodeHeaderSize     uint32 = commonNodeHeaderSize + leafNodeNumCellsSize + leafNodeNextLeafSize
)

// Leaf Node Body Layout
const (
	leafNodeKeySize     uint32 = 4
	leafNodeKeyOffset   uint32 = 0
	leafNodeValueSize   uint32 = rowSize
	leafNodeValueOffset uint32 = leafNodeKeyOffset + leafNodeKeySize
	leafNodeCellSize    uint32 = leafNodeKeySize + leafNodeValueSize
	leafNodeCellSpace   uint32 = pageSize - leafNodeHeaderSize
	leafNodeMaxCells    uint32 = leafNodeCellSpace / leafNodeCellSize
)

// Internal node header layout
const (
	internalNodeNumKeysSize      uint32 = 4
	internalNodeNumKeysOffset    uint32 = commonNodeHeaderSize
	internalNodeRightChildSize   uint32 = 4
	internalNodeRightChildOffset uint32 = internalNodeNumKeysOffset + internalNodeRightChildSize
	internalNodeHeaderSize       uint32 = commonNodeHeaderSize + internalNodeNumKeysSize + internalNodeRightChildSize
)

// Internal Node Body Layout
const (
	internalNodeKeySize   uint32 = 4
	internalNodeChildSize uint32 = 4
	internalNodeCellSize  uint32 = internalNodeChildSize + internalNodeKeySize
	// TODO making count small for testing
	internalNodeMaxCells uint32 = 3
)

const (
	leafNodeRightSplitCount uint32 = (leafNodeMaxCells + 1) / 2
	leafNodeLeftSplitCount  uint32 = (leafNodeMaxCells + 1) - leafNodeRightSplitCount
)

func getNodeParent(page []byte) uint32 {
	return binary.LittleEndian.Uint32(
		page[parentPointerOffset : parentPointerOffset+parentPointerSize])
}

func setNodeParent(page []byte, parent uint32) {
	binary.LittleEndian.PutUint32(
		page[parentPointerOffset:parentPointerOffset+parentPointerSize],
		parent)
}

func getLeafNodeNumCells(page []byte) uint32 {
	return binary.LittleEndian.Uint32(page[leafNodeNumCellsOffset : leafNodeNumCellsOffset+leafNodeNumCellsSize])
}

func setLeafNodeNumCells(page []byte, value uint32) {
	binary.LittleEndian.PutUint32(page[leafNodeNumCellsOffset:leafNodeNumCellsOffset+leafNodeNumCellsSize], value)
}

func incrementLeafNodeNumCells(page []byte) {
	num := binary.LittleEndian.Uint32(page[leafNodeNumCellsOffset : leafNodeNumCellsOffset+leafNodeNumCellsSize])
	setLeafNodeNumCells(page, num+1)
}

func getleafNodeCell(page []byte, cellNum uint32) []byte {
	// TODO add bounds checking
	offset := leafNodeHeaderSize + (cellNum * leafNodeCellSize)
	return page[offset : offset+leafNodeCellSize]
}

func getLeafNodeKey(page []byte, cellNum uint32) uint32 {
	cell := getleafNodeCell(page, cellNum)
	return binary.LittleEndian.Uint32(cell[leafNodeKeyOffset : leafNodeKeyOffset+leafNodeKeySize])
}

func setLeafNodeKey(page []byte, cellNum, key uint32) {
	cell := getleafNodeCell(page, cellNum)
	binary.LittleEndian.PutUint32(cell[leafNodeKeyOffset:leafNodeKeyOffset+leafNodeKeySize], key)
}

func getLeafNodeValue(page []byte, cellNum uint32) []byte {
	cell := getleafNodeCell(page, cellNum)
	return cell[leafNodeValueOffset : leafNodeValueOffset+leafNodeValueSize]
}

func getLeafNodeNextLeaf(page []byte) uint32 {
	return binary.LittleEndian.Uint32(
		page[leafNodeNextLeafOffset : leafNodeNextLeafOffset+leafNodeNextLeafSize])
}

func setLeafNodeNextLeaf(page []byte, nextLeaf uint32) {
	binary.LittleEndian.PutUint32(
		page[leafNodeNextLeafOffset:leafNodeNextLeafOffset+leafNodeNextLeafSize],
		nextLeaf)
}

// initializeLeafNode modifies the page passed
// in to the fucntion, adding the required header values
func initializeLeafNode(page []byte) {
	setNodeType(page, leafNode)
	setNodeRoot(page, false)
	setLeafNodeNextLeaf(page, 0)
	binary.LittleEndian.PutUint32(page[leafNodeNumCellsOffset:leafNodeNumCellsOffset+leafNodeNumCellsSize], 0)
}

// This modifies the page
func leafNodeInsert(cursor *Cursor, key uint32, value serializedRow) error {
	node, err := cursor.table.pager.GetPage(cursor.pageNum)
	if err != nil {
		return err
	}

	numCells := getLeafNodeNumCells(node)
	if numCells >= leafNodeMaxCells {
		return leafNodeSplitAndInsert(cursor, key, value)
	}

	if cursor.cellNum < numCells {
		for i := numCells; i > cursor.cellNum; i-- {
			copy(getleafNodeCell(node, i), getleafNodeCell(node, i-1))
		}
	}

	incrementLeafNodeNumCells(node)
	setLeafNodeKey(node, cursor.cellNum, key)
	copy(getLeafNodeValue(node, cursor.cellNum), value)

	return nil
}

func leafNodeSplitAndInsert(c *Cursor, key uint32, value serializedRow) error {
	oldNode, err := c.table.pager.GetPage(c.pageNum)
	if err != nil {
		return err
	}

	oldMax := getNodeMaxKey(oldNode)

	newPageNum := c.table.pager.GetUnusedPageNum()
	newNode, err := c.table.pager.GetPage(newPageNum)
	if err != nil {
		return err
	}

	initializeLeafNode(newNode)
	setNodeParent(newNode, getNodeParent(oldNode))

	setLeafNodeNextLeaf(newNode, getLeafNodeNextLeaf(oldNode))
	setLeafNodeNextLeaf(oldNode, newPageNum)

	var destinationNode []byte
	for i := int(leafNodeMaxCells); i >= 0; i-- {
		if uint32(i) >= leafNodeLeftSplitCount {
			destinationNode = newNode
		} else {
			destinationNode = oldNode
		}

		indexWithinNode := uint32(i) % leafNodeLeftSplitCount

		if uint32(i) == c.cellNum {
			copy(getLeafNodeValue(destinationNode, indexWithinNode), value)
			setLeafNodeKey(destinationNode, indexWithinNode, key)
		} else if uint32(i) > c.cellNum {
			dest := getleafNodeCell(destinationNode, indexWithinNode)
			copy(dest, getleafNodeCell(oldNode, uint32(i-1)))
		} else {
			dest := getleafNodeCell(destinationNode, indexWithinNode)
			copy(dest, getleafNodeCell(oldNode, uint32(i)))
		}
	}

	setLeafNodeNumCells(oldNode, leafNodeRightSplitCount)
	setLeafNodeNumCells(newNode, leafNodeLeftSplitCount)

	if isNodeRoot(oldNode) {
		return createNewRoot(c.table, newPageNum)
	}

	parentPageNum := getNodeParent(oldNode)
	newMax := getNodeMaxKey(oldNode)

	parent, err := c.table.pager.GetPage(parentPageNum)
	if err != nil {
		return err
	}
	updateInternalNodeKey(parent, oldMax, newMax)
	internalNodeInsert(c.table, parentPageNum, newPageNum)

	return nil
}

// TODO, not thrilled about passing the table as a parameter here
// find a better way to do this
func internalNodeInsert(table *Table, parentPageNum, childPageNum uint32) error {
	parent, err := table.pager.GetPage(parentPageNum)
	if err != nil {
		return err
	}

	child, err := table.pager.GetPage(childPageNum)
	if err != nil {
		return err
	}

	childMaxKey := getNodeMaxKey(child)
	index := internalNodeFindChild(parent, childMaxKey)

	origNumKeys := getInternalNodeNumKeys(parent)
	setInternalNodeNumKeys(parent, origNumKeys+1)

	if origNumKeys >= internalNodeMaxCells {
		return errors.New("need to implement splitting internal node")
	}

	rightChildPageNum := getInternalNodeRightChild(parent)
	rightChild, err := table.pager.GetPage(rightChildPageNum)
	if err != nil {
		return err
	}

	if childMaxKey > getNodeMaxKey(rightChild) {
		setInternalNodeChild(parent, origNumKeys, rightChildPageNum)
		setInternalNodeRightChild(parent, childPageNum)

		return nil
	}

	for i := origNumKeys; i > index; i-- {
		dest := getInternalNodeCell(parent, i)
		source := getInternalNodeCell(parent, i-1)
		copy(dest, source)
	}

	setInternalNodeChild(parent, index, childPageNum)
	setInternalNodeKey(parent, index, childMaxKey)

	return nil
}

func updateInternalNodeKey(page []byte, oldKey, newKey uint32) {
	oldChildIndex := internalNodeFindChild(page, oldKey)
	setInternalNodeKey(page, oldChildIndex, newKey)
}

func internalNodFindChild(page []byte)

func createNewRoot(t *Table, rightChildPageNum uint32) error {
	root, err := t.pager.GetPage(t.rootPageNum)
	if err != nil {
		return err
	}

	rightChild, err := t.pager.GetPage(rightChildPageNum)
	if err != nil {
		return err
	}

	leftChildPageNum := t.pager.GetUnusedPageNum()
	leftChild, err := t.pager.GetPage(leftChildPageNum)
	if err != nil {
		return err
	}

	copy(leftChild, root)
	setNodeRoot(leftChild, false)

	initializeInternalNode(root)
	setNodeRoot(root, true)

	setInternalNodeNumKeys(root, 1)
	setInternalNodeChild(root, 0, leftChildPageNum)

	leftChildMaxKey := getNodeMaxKey(leftChild)
	setInternalNodeKey(root, 0, leftChildMaxKey)
	setInternalNodeRightChild(root, rightChildPageNum)

	setNodeParent(leftChild, t.rootPageNum)
	setNodeParent(rightChild, t.rootPageNum)

	return nil
}

func getNodeType(page []byte) NodeType {
	var nodeType NodeType

	buf := bytes.NewReader(page[nodeTypeOffset : nodeTypeOffset+nodeTypeSize])
	err := binary.Read(buf, binary.LittleEndian, &nodeType)
	if err != nil {
		panic(err)
	}

	return nodeType
}

func setNodeType(page []byte, nodeType NodeType) {
	err := binary.Write(ArrayWriter(page[nodeTypeOffset:nodeTypeOffset+nodeTypeSize]), binary.LittleEndian, nodeType)
	if err != nil {
		panic(err)
	}
}

func setInternalNodeNumKeys(page []byte, numKeys uint32) {
	binary.LittleEndian.PutUint32(
		page[internalNodeNumKeysOffset:internalNodeNumKeysOffset+internalNodeNumKeysSize],
		numKeys)
}

func getInternalNodeNumKeys(page []byte) uint32 {
	return binary.LittleEndian.Uint32(
		page[internalNodeNumKeysOffset : internalNodeNumKeysOffset+internalNodeNumKeysSize],
	)
}

// setInternalNodeRightChild sets the rightmost child of the internal node
// the pointer of which is in the header
func setInternalNodeRightChild(page []byte, pageNum uint32) {
	binary.LittleEndian.PutUint32(
		page[internalNodeRightChildOffset:internalNodeRightChildOffset+internalNodeRightChildSize],
		pageNum)
}

func setInternalNodeChild(page []byte, childNum, newChildNum uint32) error {
	numKeys := getInternalNodeNumKeys(page)
	if childNum > numKeys {
		return fmt.Errorf("Tried to access child num %d > num_keys %d\n", childNum, numKeys)
	}

	if childNum == numKeys {
		setInternalNodeRightChild(page, newChildNum)
		return nil
	}

	cell := getInternalNodeCell(page, childNum)

	binary.LittleEndian.PutUint32(cell[:internalNodeChildSize], newChildNum)
	return nil
}

func setInternalNodeKey(page []byte, keyNum, key uint32) {
	cell := getInternalNodeCell(page, keyNum)
	binary.LittleEndian.PutUint32(cell[internalNodeChildSize:internalNodeChildSize+internalNodeKeySize], key)
}

func getInternalNodeCell(page []byte, cellNum uint32) []byte {
	offset := internalNodeHeaderSize + cellNum*internalNodeCellSize
	return page[offset : offset+internalNodeCellSize]
}

func getInternalNodeKey(page []byte, keyNum uint32) uint32 {
	cell := getInternalNodeCell(page, keyNum)
	return binary.LittleEndian.Uint32(cell[internalNodeChildSize : internalNodeChildSize+internalNodeKeySize])
}

func getNodeMaxKey(page []byte) uint32 {
	switch getNodeType(page) {
	case internalNode:
		return getInternalNodeKey(page, getInternalNodeNumKeys(page)-1)

	case leafNode:
		return getLeafNodeKey(page, getLeafNodeNumCells(page)-1)

	default:
		panic("Node type not recognized, the page may have been corrupted.")
	}
}

func isNodeRoot(page []byte) bool {
	var isRoot bool

	buf := bytes.NewReader(page[isRootOffset : isRootOffset+isRootSize])
	err := binary.Read(buf, binary.LittleEndian, &isRoot)
	if err != nil {
		panic(err)
	}

	return isRoot
}

func setNodeRoot(page []byte, isRoot bool) {
	err := binary.Write(ArrayWriter(page[isRootOffset:isRootOffset+isRootSize]), binary.LittleEndian, isRoot)
	if err != nil {
		panic(err)
	}
}

func initializeInternalNode(page []byte) {
	setNodeType(page, internalNode)
	setNodeRoot(page, false)
	setInternalNodeNumKeys(page, 0)
}

func getInternalNodeChild(page []byte, childNum uint32) (uint32, error) {
	numKeys := getInternalNodeNumKeys(page)
	if childNum > numKeys {
		return 0, fmt.Errorf("Tried to access child num %d > num_keys %d\n", childNum, numKeys)
	}

	if childNum == numKeys {
		return getInternalNodeRightChild(page), nil
	}

	cell := getInternalNodeCell(page, childNum)
	return binary.LittleEndian.Uint32(cell[:internalNodeChildSize]), nil
}

// getInternalNodeRightChild gets the rightmost child of the internal node
// the pointer of which is in the header
func getInternalNodeRightChild(page []byte) uint32 {
	return binary.LittleEndian.Uint32(page[internalNodeRightChildOffset : internalNodeRightChildOffset+internalNodeRightChildSize])
}
