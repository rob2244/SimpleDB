package persist

import "encoding/binary"

type NodeType uint8

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
	leafNodeHeaderSize     uint32 = commonNodeHeaderSize + leafNodeNumCellsSize
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

func leafNodeNumCells(page []byte) uint32 {
	return binary.LittleEndian.Uint32(page[leafNodeNumCellsOffset : leafNodeNumCellsOffset+leafNodeNumCellsSize])
}

func leafNodeCell(page []byte, cellNum uint32) []byte {
	// TODO add bounds checking
	offset := leafNodeHeaderSize + (cellNum * leafNodeCellSize)
	return page[offset : offset+leafNodeCellSize]
}

func leafNodeKey(page []byte, cellNum uint32) uint32 {
	cell := leafNodeCell(page, cellNum)
	return binary.LittleEndian.Uint32(cell[leafNodeKeyOffset : leafNodeKeyOffset+leafNodeKeySize])
}

func leafNodeValue(page []byte, cellNum uint32) []byte {
	cell := leafNodeCell(page, cellNum)
	return cell[leafNodeValueOffset : leafNodeValueOffset+leafNodeValueSize]
}
