package main

import (
	"fmt"
	"os"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/apache/arrow/go/v13/parquet"
	"github.com/apache/arrow/go/v13/parquet/compress"
	"github.com/apache/arrow/go/v13/parquet/pqarrow"
)

const (
	defaultRowGroupByteLimit = 10 * 1024 * 1024
)

var (
	defaultWrtp = parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Snappy),
		parquet.WithStats(true),
		parquet.WithRootName("arrow_schema"),
	)
)

type ParquetWriter struct {
	destFile *os.File
	pqwrt    *pqarrow.FileWriter
	sc       *arrow.Schema
	count    int
}

//	NewParquetWriter creates a new ParquetWriter.
//
// sc is the Arrow schema to use for writing records.
// wrtp are the Parquet writer properties to use.
//
// Returns a ParquetWriter and an error. The error will be non-nil if:
// - Failed to get the Parquet schema from the Arrow schema.
// - Failed to create the destination file.
// - Failed to create the Parquet file writer.
//
// Example:
// ```go
// pw, err := NewParquetWriter(schema, parquet.NewWriterProperties(parquet.WithCompression(parquet.CompressionCodec_SNAPPY)))
//
//	if err != nil {
//	  log.Fatal(err)
//	}
//
// ```
func NewParquetWriter(sc *arrow.Schema, wrtp *parquet.WriterProperties) (*ParquetWriter, error) {
	pqschema, err := pqarrow.ToParquet(sc, wrtp, pqarrow.DefaultWriterProps())
	if err != nil {
		return nil, fmt.Errorf("failed to get parquet schema: %w", err)
	}

	fmt.Println(pqschema.String())

	destFile, err := os.Create(flags.Destination)
	if err != nil {
		return nil, fmt.Errorf("failed to create destination file: %w", err)
	}

	pqwrt, err := pqarrow.NewFileWriter(sc, destFile, wrtp, pqarrow.DefaultWriterProps())
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet writer: %w", err)
	}

	return &ParquetWriter{destFile: destFile, pqwrt: pqwrt, sc: sc}, nil
}

//	Write writes a single record to the Parquet file.
//
// jsonData is the JSON encoded record data.
//
// Returns an error if:
// - Failed to unmarshal the JSON data.
// - Failed to write the record to Parquet.
//
// Increments the record count and creates a new row group if the current
// row group exceeds the default row group byte limit.
//
// Example:
// ```go
// err := pw.Write([]byte(`{"id":1,"name":"foo"}`))
//
//	if err != nil {
//	  log.Fatal(err)
//	}
//
// ```
func (pw *ParquetWriter) Write(jsonData []byte) error {
	recbld := array.NewRecordBuilder(memory.DefaultAllocator, pw.sc)
	defer recbld.Release()

	err := recbld.UnmarshalJSON(jsonData)
	if err != nil {
		return fmt.Errorf("failed to unmarshal JSON: %w", err)
	}

	rec := recbld.NewRecord()
	defer rec.Release()
	err = pw.pqwrt.WriteBuffered(rec)
	if err != nil {
		return fmt.Errorf("failed to write to parquet: %w", err)
	}

	if pw.pqwrt.RowGroupTotalBytesWritten() >= defaultRowGroupByteLimit {
		pw.pqwrt.NewBufferedRowGroup()
	}
	pw.count++

	return nil
}

// RecordCount returns the total number of records written.
func (pw *ParquetWriter) RecordCount() int {
	return pw.count
}

//	Close closes the Parquet writer.
//
// Returns an error if failed to close the Parquet file writer.
func (pw *ParquetWriter) Close() error {
	if err := pw.pqwrt.Close(); err != nil {
		return fmt.Errorf("failed to close parquet writer: %w", err)
	}

	return nil
}
