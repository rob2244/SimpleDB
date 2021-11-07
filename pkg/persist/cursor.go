package persist

type Cursor struct {
	table      *Table
	rowNum     uint32
	endOfTable bool
}

func (c Cursor) Value() (serializedRow, error) {
	pageNum := c.rowNum / rowsPerPage
	page, err := c.table.pager.GetPage(pageNum)

	if err != nil {
		return nil, err
	}

	rowOffset := c.rowNum % rowsPerPage
	byteOffset := rowOffset * rowSize

	return serializedRow(page[byteOffset : byteOffset+rowSize]), nil
}

func (c *Cursor) Advance() {
	// TODO bounds checking here
	c.rowNum += 1

	if c.rowNum >= c.table.numRows {
		c.endOfTable = true
	}
}

func TableStart(t *Table) *Cursor {
	return &Cursor{
		table:      t,
		rowNum:     0,
		endOfTable: t.numRows == 0,
	}
}

func TableEnd(t *Table) *Cursor {
	return &Cursor{
		table:      t,
		rowNum:     t.numRows,
		endOfTable: true,
	}
}
