package persist

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"unicode"

	"github.com/fatih/color"
)

// All fields will be fixed in length initially.
// These constants represent the size and offsets of fixed fields in bytes
// The offset is calculated from the beggining of the message not the beggning
// of the last field
const (
	idSize         uint32 = 4
	idOffset       uint32 = 0
	usernameSize   uint32 = 32
	usernameOffset uint32 = 4
	emailSize      uint32 = 255
	emailOffset    uint32 = 36
	rowSize        uint32 = 291
)

// Constants for the in memory table definition
const (
	pageSize      uint32 = 4096
	tableMaxPages uint32 = 100
	rowsPerPage   uint32 = pageSize / rowSize
	tableMaxRows  uint32 = rowsPerPage * tableMaxPages
)

type serializedRow []byte

func (b serializedRow) Deserialize() *Row {
	id := binary.LittleEndian.Uint32(b[idOffset : idOffset+idSize])
	username := string(bytes.TrimRight(b[usernameOffset:usernameOffset+usernameSize], "\x00"))
	email := string(bytes.TrimRight(b[emailOffset:emailOffset+emailSize], "\x00"))

	return &Row{id: id, username: username, email: email}
}

type Row struct {
	id       uint32
	username string
	email    string
}

func (r Row) String() string {
	return fmt.Sprintf("(%d, %s, %s)", r.id, r.username, r.email)
}

func (r *Row) Serialize() (serializedRow, error) {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, r.id)

	result := make([]byte, rowSize)

	copy(result[idOffset:idOffset+idSize], buf)
	copy(result[usernameOffset:usernameOffset+usernameSize], []byte(r.username))
	copy(result[emailOffset:emailOffset+emailSize], []byte(r.email))

	return result, nil
}

func isAscii(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] > unicode.MaxASCII {
			return false
		}
	}

	return true
}

func NewRow(id uint32, username string, email string) (*Row, error) {
	if !isAscii(username) || len(username) > 32 {
		return nil, fmt.Errorf(
			"invalid username value. username must use ascii characters only and have a maximum of 32 characters",
		)
	}

	if !isAscii(email) || len(email) > 255 {
		return nil, fmt.Errorf(
			"invalid username value. username must use ascii characters only and have a maximum of 32 characters",
		)
	}

	return &Row{
		id:       id,
		username: username,
		email:    email,
	}, nil
}

type Table struct {
	rootPageNum uint32
	pager       *pager
}

func (t *Table) Select() error {
	c, err := TableStart(t)
	if err != nil {
		return err
	}

	for !c.endOfTable {
		v, err := c.Value()
		if err != nil {
			return err
		}

		fmt.Println(v.Deserialize())
		c.Advance()
	}

	return nil
}

func (t Table) PrintTree(pageNum uint32, indentationLevel int) error {
	page, err := t.pager.GetPage(pageNum)

	if err != nil {
		return err
	}

	switch getNodeType(page) {
	case leafNode:
		numKeys := getLeafNodeNumCells(page)
		indent(indentationLevel)
		fmt.Printf("- leaf (size %d)\n", numKeys)

		for i := 0; i < int(numKeys); i++ {
			indent(indentationLevel + 1)
			fmt.Printf("- %d\n", getLeafNodeKey(page, uint32(i)))
		}
		return nil

	case internalNode:
		numKeys := getInternalNodeNumKeys(page)
		indent(indentationLevel)

		fmt.Printf("- internal (size %d)\n", numKeys)
		for i := 0; i < int(numKeys); i++ {
			child, err := getInternalNodeChild(page, uint32(i))
			if err != nil {
				return err
			}

			t.PrintTree(child, indentationLevel+1)

			indent(indentationLevel + 1)
			fmt.Printf("- key %d\n", getInternalNodeKey(page, uint32(i)))
		}

		child := getInternalNodeRightChild(page)
		t.PrintTree(child, indentationLevel+1)

		return nil

	default:
		panic("Node type not recognized, the page may have been corrupted.")
	}
}

func indent(level int) {
	for i := 0; i < level; i++ {
		fmt.Print("  ")
	}
}

func (t *Table) Insert(r *Row) error {
	n, err := t.pager.GetPage(t.rootPageNum)
	if err != nil {
		return err
	}

	keyToInsert := r.id
	c, err := TableFind(t, keyToInsert)
	if err != nil {
		return err
	}

	if c.cellNum < getLeafNodeNumCells(n) {
		keyAtIdx := getLeafNodeKey(n, c.cellNum)
		if keyAtIdx == keyToInsert {
			return fmt.Errorf("duplicate key found '%d'", keyToInsert)
		}
	}

	serialized, err := r.Serialize()
	if err != nil {
		return err
	}

	err = leafNodeInsert(c, r.id, serialized)
	if err != nil {
		return err
	}

	return nil
}

func (t *Table) Close() error {
	for i := 0; i < int(t.pager.numPages); i++ {
		if t.pager.pages[i] != nil {
			if err := t.pager.FlushPage(uint32(i)); err != nil {
				return err
			}
		}
	}

	return t.pager.Close()
}

func OpenDatabase(filename string) (*Table, error) {
	pager, err := NewPager(filename)

	if err != nil {
		return nil, err
	}

	if pager.numPages == 0 {
		root, err := pager.GetPage(0)

		if err != nil {
			return nil, err
		}

		initializeLeafNode(root)
		setNodeRoot(root, true)
	}

	return &Table{
		rootPageNum: 0,
		pager:       pager,
	}, nil
}

func PrintConstants() {
	color.Green("ROW_SIZE: %d\n", rowSize)
	color.Green("COMMON_NODE_HEADER_SIZE: %d\n", commonNodeHeaderSize)
	color.Green("LEAF_NODE_HEADER_SIZE: %d\n", leafNodeHeaderSize)
	color.Green("LEAF_NODE_CELL_SIZE: %d\n", leafNodeCellSize)
	color.Green("LEAF_NODE_SPACE_FOR_CELLS: %d\n", leafNodeCellSpace)
	color.Green("LEAF_NODE_MAX_CELLS: %d\n", leafNodeMaxCells)
}
