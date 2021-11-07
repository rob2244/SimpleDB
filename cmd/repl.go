package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/fatih/color"
	"github.com/rob2244/SimpleDB/pkg/persist"
)

type statementType int

const (
	statementInsert statementType = iota
	statementSelect
)

type statement struct {
	statementType statementType
	rowToInsert   *persist.Row
}

func main() {
	reader := bufio.NewReader(os.Stdin)
	t, err := persist.OpenDatabase("test.db")
	if err != nil {
		color.Red("Unable to open database: '%s'", err)
	}
	defer t.Close()

	for {
		printPrompt()
		input := readInput(reader)

		if strings.HasPrefix(input, ".") {
			if err := doMetaCommand(input, t); err != nil {
				color.Yellow("%v'\n", err)
			}

			continue
		}

		statement, err := prepareStatement(input)
		if err != nil {
			color.Yellow("%v\n", err)
			continue
		}

		executeStatement(statement, t)
		color.Green("Executed.\n")
	}
}

func printPrompt() {
	fmt.Print("db > ")
}

func readInput(reader *bufio.Reader) string {
	text, err := reader.ReadString('\n')
	if err != nil {
		color.Red("Unexpected error while reading input %s\n", err)
		os.Exit(1)
	}

	return strings.Replace(text, "\n", "", -1)
}

func doMetaCommand(input string, t *persist.Table) error {
	if strings.Compare(input, ".exit") == 0 {
		t.Close()
		os.Exit(0)
	} else {
		return fmt.Errorf("unrecognized keyword at start of '%s'", input)
	}

	return nil
}

func prepareStatement(input string) (*statement, error) {
	if strings.HasPrefix(input, "insert") {
		strs := strings.Split(input, " ")[1:]

		if len(strs) != 3 {
			return nil, fmt.Errorf("syntax error in insert command '%s'", input)
		}

		n, err := strconv.Atoi(strs[0])
		if err != nil || n < 0 {
			return nil, fmt.Errorf("invalid user id: '%s'", strs[0])
		}

		row, err := persist.NewRow(uint32(n), strs[1], strs[2])
		if err != nil {
			return nil, err
		}

		return &statement{
				statementType: statementInsert,
				rowToInsert:   row},
			nil
	}

	if strings.HasPrefix(input, "select") {
		return &statement{statementType: statementSelect}, nil
	}

	return nil, fmt.Errorf("unrecognized command '%s'", input)
}

func executeStatement(stmnt *statement, t *persist.Table) {
	switch stmnt.statementType {
	case statementInsert:
		if err := executeInsert(stmnt, t); err != nil {
			color.Red("Insert failed: '%v'", err)
			return
		}

		color.Green("Inserting into database")
		return

	case statementSelect:
		executeSelect(stmnt, t)
		color.Green("Rows retrieved successfully")

		return
	}
}

func executeInsert(stmnt *statement, t *persist.Table) error {
	serialized, err := stmnt.rowToInsert.Serialize()
	if err != nil {
		return err
	}

	return t.Insert(serialized)
}

func executeSelect(stmnt *statement, t *persist.Table) {
	t.Select()
}
