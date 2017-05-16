package linefile

import (
	"bufio"
	"context"
	"os"
	"strconv"
)

func New(filename string) *LineFile {
	return &LineFile{
		filename: filename,
	}
}

type LineFile struct {
	f        *os.File
	r        *bufio.Reader
	pos      int
	filename string
}

func (lf *LineFile) Next(context.Context) (string, interface{}, error) {
	l, err := lf.r.ReadString('\n')
	if err != nil {
		return "", nil, err
	}
	pos := lf.pos
	lf.pos++
	return strconv.Itoa(pos), l, nil
}

func (lf *LineFile) Name() string {
	return lf.filename
}

func (lf *LineFile) Init() error {
	f, err := os.Open(lf.filename)
	if err != nil {
		return err
	}
	lf.f = f
	lf.r = bufio.NewReader(f)
	return nil
}

func (lf *LineFile) Close() error {
	return lf.f.Close()
}
