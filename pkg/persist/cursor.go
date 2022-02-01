package persist

type Cursor struct {
	table      *Table
	pageNum    uint32
	cellNum    uint32
	endOfTable bool
}

func (c Cursor) Value() (serializedRow, error) {
	page, err := c.table.pager.GetPage(c.pageNum)

	if err != nil {
		return nil, err
	}

	return serializedRow(getLeafNodeValue(page, c.cellNum)), nil
}

func (c *Cursor) Advance() error {
	page, err := c.table.pager.GetPage(c.pageNum)

	if err != nil {
		return err
	}

	c.cellNum += 1

	if c.cellNum >= getLeafNodeNumCells(page) {
		nextPageNum := getLeafNodeNextLeaf(page)
		if nextPageNum == 0 {
			c.endOfTable = true
		} else {
			c.pageNum = nextPageNum
			c.cellNum = 0
		}
	}

	return nil
}

func TableStart(t *Table) (*Cursor, error) {
	cursor, err := TableFind(t, 0)
	if err != nil {
		return nil, err
	}

	page, err := t.pager.GetPage(cursor.pageNum)
	if err != nil {
		return nil, err
	}

	numCells := getLeafNodeNumCells(page)
	cursor.endOfTable = (numCells == 0)

	return cursor, nil
}

// TableFind returns a cursor pointing to the position of
// the given key. If the key is not present, return the position
// where it should be inserted
func TableFind(t *Table, key uint32) (*Cursor, error) {
	n, err := t.pager.GetPage(t.rootPageNum)
	if err != nil {
		return nil, err
	}

	if getNodeType(n) == leafNode {
		return leafNodeFind(t, t.rootPageNum, key)
	} else {
		return internalNodeFind(t, t.rootPageNum, key)
	}
}

func internalNodeFind(t *Table, pageNum, key uint32) (*Cursor, error) {
	page, err := t.pager.GetPage(pageNum)
	if err != nil {
		return nil, err
	}

	childIndex := internalNodeFindChild(page, key)
	childNum, err := getInternalNodeChild(page, childIndex)
	if err != nil {
		return nil, err
	}

	child, err := t.pager.GetPage(childNum)
	if err != nil {
		return nil, err
	}

	switch getNodeType(child) {
	case leafNode:
		return leafNodeFind(t, childNum, key)

	case internalNode:
		return internalNodeFind(t, childNum, key)

	default:
		panic("Node type not recognized, the page may have been corrupted.")

	}
}

// TODO, this function is also used in btree.go
// I don't like that it's used in both places/how it's structured
// fix this
func internalNodeFindChild(page []byte, key uint32) uint32 {
	numKeys := getInternalNodeNumKeys(page)

	low := 0
	// There is one more child than key
	high := int(numKeys)

	for low != high {
		mid := low + ((high - low) / 2)
		keyToRight := getInternalNodeKey(page, uint32(mid))

		if keyToRight >= key {
			high = mid
		} else {
			low = mid + 1
		}
	}

	return uint32(low)
}

func leafNodeFind(t *Table, pageNum, key uint32) (*Cursor, error) {
	n, err := t.pager.GetPage(pageNum)
	if err != nil {
		return nil, err
	}

	numCells := getLeafNodeNumCells(n)

	low := 0
	high := int(numCells)

	for low < high {
		mid := low + ((high - low) / 2)
		keyAtIdx := getLeafNodeKey(n, uint32(mid))

		if key == keyAtIdx {
			return &Cursor{
				table:      t,
				pageNum:    pageNum,
				cellNum:    uint32(mid),
				endOfTable: true,
			}, nil
		} else if key < keyAtIdx {
			high = mid - 1
		} else {
			low = mid + 1
		}
	}

	return &Cursor{
		table:      t,
		pageNum:    pageNum,
		cellNum:    uint32(low),
		endOfTable: true,
	}, nil

}
