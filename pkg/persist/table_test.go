package persist

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"testing"
)

var testDirPath string = "test_data"

func TestInsertAndRetrieve(t *testing.T) {
	createTestDir(t, testDirPath)
	t.Cleanup(cleanupTestDir(t, testDirPath))

	tbl, err := OpenDatabase(path.Join(testDirPath, "test.db"))
	if err != nil {
		t.Fatalf("%s", err)
	}

	row, err := NewRow(33, "test", "testtesterson@gmail.com")
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
	// This will return an error if the close method down below
	// is called. This isn't a problem as the file handle gets closed.
	defer w.Close()

	stdOut := os.Stdout
	os.Stdout = w

	if err := tbl.Insert(row); err != nil {
		t.Fatalf("%s", err)
	}

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

func TestBTreeNodeSplit(t *testing.T) {
	createTestDir(t, testDirPath)
	t.Cleanup(cleanupTestDir(t, testDirPath))

	tbl, err := OpenDatabase(path.Join(testDirPath, "test.db"))
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
	// This will return an error if the close method down below
	// is called. This isn't a problem as the file handle gets closed.
	defer w.Close()

	stdOut := os.Stdout
	os.Stdout = w

	for i := 0; i < 15; i++ {
		row, err := NewRow(uint32(i), fmt.Sprintf("user#%d", i), fmt.Sprintf("person#%d@example.com", i))
		if err != nil {
			t.Fatalf("Unable to create row: '%s'", err)
		}

		if err := tbl.Insert(row); err != nil {
			t.Fatalf("Unable to insert row: '%s'", err)
		}
	}

	if err := tbl.PrintTree(0, 0); err != nil {
		t.Fatalf("Unable to print tree: '%s'", err)
	}

	if err := tbl.Select(); err != nil {
		t.Fatalf("Unable to select data: '%s'", err)
	}

	if err = w.Close(); err != nil {
		t.Fatalf("%s", err)
	}

	out, err := ioutil.ReadAll(r)
	if err != nil {
		t.Fatalf("%s", err)
	}

	os.Stdout = stdOut
	so := string(out)

	expected := `
- internal (size 1)
  - leaf (size 7)
	- 0
	- 1
	- 2
	- 3
	- 4
	- 5
	- 6
  - key 6
  - leaf (size 8)
    - 7
	- 8
	- 9
	- 10
	- 11
	- 12
	- 13
	- 14
	`

	fmt.Print(so)

	if so != expected {
		t.Fail()
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

func createTestDir(t *testing.T, dirPath string) {
	if err := os.Mkdir(dirPath, os.FileMode(0777)); err != nil {
		t.Fatalf("Unable to create testing directory '%s'. Aborting...", err)
	}
}

func cleanupTestDir(t *testing.T, dirPath string) func() {
	return func() {
		if err := os.RemoveAll(testDirPath); err != nil {
			t.Logf("Unable to delete test directory: '%s'", dirPath)
		}
	}
}
