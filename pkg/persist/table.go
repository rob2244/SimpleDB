package persist

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"unicode"
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
	numRows uint32
	pager   *pager
}

func (t Table) isFull() bool {
	return t.numRows >= tableMaxRows
}

func (t *Table) Select() error {
	c := TableStart(t)

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

func (t *Table) Insert(r serializedRow) error {
	if t.isFull() {
		return errors.New("no space left in table")
	}

	c := TableEnd(t)

	s, err := c.Value()
	if err != nil {
		return err
	}

	copy(s, r)

	t.numRows++
	return nil
}

func (t *Table) Close() error {
	fullPages := t.numRows / rowsPerPage

	for i := 0; i < int(fullPages); i++ {
		if t.pager.pages[i] != nil {
			if err := t.pager.FlushPage(uint32(i), pageSize); err != nil {
				return err
			}
		}
	}

	numAddtlRows := t.numRows % rowsPerPage
	if numAddtlRows != 0 {
		if err := t.pager.FlushPage(fullPages, numAddtlRows*rowSize); err != nil {
			return err
		}
	}

	return t.pager.Close()
}

func OpenDatabase(filename string) (*Table, error) {
	pager, err := NewPager(filename)

	if err != nil {
		return nil, err
	}

	return &Table{
		numRows: pager.fileLength / rowSize,
		pager:   pager,
	}, nil
}
