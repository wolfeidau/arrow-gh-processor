package main

import (
	"bufio"
	"io"

	gzip "github.com/klauspost/pgzip"
)

type GzipJSONReader struct {
	rdr       *gzip.Reader
	linrdr    *bufio.Reader
	count     int
	bytesRead int64
}

func NewGzipJSONReader(r io.Reader) (*GzipJSONReader, error) {
	gzipReader, err := gzip.NewReader(r)
	if err != nil {
		return nil, err
	}

	return &GzipJSONReader{
		rdr:    gzipReader,
		linrdr: bufio.NewReader(gzipReader),
	}, nil
}

func (gzr *GzipJSONReader) ReadLine() ([]byte, error) {
	data, err := gzr.linrdr.ReadBytes('\n')
	if err != nil {
		return nil, err
	}

	gzr.count++
	gzr.bytesRead += int64(len(data))
	return data, nil
}

func (gzr *GzipJSONReader) BytesRead() int64 {
	return gzr.bytesRead
}

func (gzr *GzipJSONReader) LineCount() int {
	return gzr.count
}

func (gzr *GzipJSONReader) Close() error {
	return gzr.rdr.Close()
}
