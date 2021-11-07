package persist

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"
)

func TestInsertAndRetrieve(t *testing.T) {
	tbl := OpenDatabase()

	row, err := NewRow(33, "test", "testtesterson@gmail.com")
	if err != nil {
		t.Fatalf("%s", err)
	}

	sr, err := row.Serialize()
	if err != nil {
		t.Fatalf("%s", err)
	}

	var r *os.File
	var w *os.File

	r, w, err = os.Pipe()

	if err != nil {
		t.Fatalf("%s", err)
	}

	defer r.Close()

	stdOut := os.Stdout
	os.Stdout = w

	tbl.Insert(sr)
	tbl.Select()

	if err = w.Close(); err != nil {
		t.Fatalf("%s", err)
	}

	out, err := ioutil.ReadAll(r)
	if err != nil {
		t.Fatalf("%s", err)
	}

	os.Stdout = stdOut

	so := string(out)
	if !strings.Contains(so, "(33, test, testtesterson@gmail.com)") {
		t.Fail()
	}
}

func TestErrorOnFullTable(t *testing.T) {
	tbl := OpenDatabase()

	for !tbl.isFull() {
		row, err := NewRow(33, "test", "testtesterson@gmail.com")
		if err != nil {
			t.Fatalf("%s", err)
		}

		sr, err := row.Serialize()
		if err != nil {
			t.Fatalf("%s", err)
		}

		tbl.Insert(sr)
	}

	row, err := NewRow(33, "test", "testtesterson@gmail.com")
	if err != nil {
		t.Fatalf("%s", err)
	}

	sr, err := row.Serialize()
	if err != nil {
		t.Fatalf("%s", err)
	}

	err = tbl.Insert(sr)

	if err == nil {
		t.Fatal("No error returned on insert when full")
	}
}

func TestAllowMaxLengthStrings(t *testing.T) {
	username := strings.Repeat("a", 32)
	email := strings.Repeat("a", 255)

	_, err := NewRow(33, username, email)

	if err != nil {
		t.Fatalf("%s", err)
	}
}

func TestErrorMessageOnStringBoundary(t *testing.T) {
	username := strings.Repeat("a", 33)

	_, err := NewRow(33, username, "testing")

	if err == nil {
		t.Fatalf("%s", err)
	}

	email := strings.Repeat("a", 256)

	_, err = NewRow(33, "testing", email)

	if err == nil {
		t.Fatalf("%s", err)
	}
}

func TestErrorOnNegativeId(t *testing.T) {
	// TODO this should go in the repl testing
}
