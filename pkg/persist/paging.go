package persist

import (
	"fmt"
	"io"
	"os"
)

type pager struct {
	fileDescriptor *os.File
	fileLength     uint32
	pages          [][]byte
}

func (p *pager) Close() error {
	return p.fileDescriptor.Close()
}

func (p *pager) FlushPage(pageNum, size uint32) error {
	if p.pages[pageNum] == nil {
		return fmt.Errorf("no page found to flush at index %d", pageNum)
	}

	_, err := p.fileDescriptor.WriteAt(p.pages[pageNum][:size], int64(pageNum)*int64(pageSize))
	return err
}

func (p *pager) GetPage(pageNum uint32) ([]byte, error) {
	if pageNum > tableMaxPages {
		return nil, fmt.Errorf("page number: '%d' out of bounds, Max page is: '%d'",
			pageNum,
			tableMaxPages)
	}

	if p.pages[pageNum] == nil {
		p.pages[pageNum] = make([]byte, pageSize)

		numPages := p.fileLength / pageSize

		if p.fileLength%pageSize != 0 {
			numPages++
		}

		if pageNum <= numPages {
			_, err := p.fileDescriptor.ReadAt(p.pages[pageNum], int64(pageNum*pageSize))
			if err != nil && err != io.EOF {
				return nil, err
			}
		}
	}

	return p.pages[pageNum], nil
}

func NewPager(filename string) (*pager, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}

	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	return &pager{
		fileDescriptor: file,
		fileLength:     uint32(stat.Size()),
		pages:          make([][]byte, tableMaxPages),
	}, nil
}
