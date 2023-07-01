package main

import (
	"bytes"
	"errors"
	"io"
	"os"
	"time"

	"github.com/alecthomas/kong"
	json "github.com/goccy/go-json"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/wolfeidau/jsontemplate"
)

var (
	version = "development"

	flags struct {
		Version     kong.VersionFlag
		Source      string `arg:"" required:"" help:"Source github archive file containing JSON and compressed with Gzip"`
		Destination string `arg:"" required:"" help:"Destination parquet output file"`
		EventType   string `enum:"PullRequestEvent" default:"PullRequestEvent"`
	}
)

type githubEvent struct {
	ID        string `json:"id,omitempty"`
	EventType string `json:"type,omitempty"`
}

func main() {
	kong.Parse(&flags,
		kong.Vars{"version": version}, // bind a var for version
		kong.Name("arrow-gh-processor"),
	)

	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	rawf, err := os.Open(flags.Source)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to open source file")
	}

	gzr, err := NewGzipJSONReader(rawf)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to open line reader")
	}

	log.Info().Str("event_type", flags.EventType).Msg("exporting events to parquet")

	pw, err := NewParquetWriter(pullRequestArrowSchema, defaultWrtp)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to open parquet writer")
	}

	ts := time.Now()

	tpl, err := jsontemplate.NewTemplate(pullRequestJSONTemplate)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to compile template")
	}

	// used to extract the event id and type
	ghe := new(githubEvent)

	for {
		lineb, err := gzr.ReadLine()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			log.Fatal().Err(err).Msg("failed to read line reader")
		}

		err = json.Unmarshal(lineb, ghe)
		if err != nil {
			log.Fatal().Err(err).Msg("failed to un marshal line")
		}

		if ghe.EventType == "PullRequestEvent" {
			buf := new(bytes.Buffer)
			_, err = tpl.Execute(buf, lineb)
			if err != nil {
				log.Fatal().Err(err).Msg("failed to execute template")
			}

			err := pw.Write(buf.Bytes())
			if err != nil {
				log.Fatal().Err(err).Msg("failed to write parquet record")
			}
		}
	}

	err = gzr.Close()
	if err != nil {
		log.Fatal().Err(err).Msg("failed to close line reader")
	}

	err = pw.Close()
	if err != nil {
		log.Fatal().Err(err).Msg("failed to close parquet writer")
	}

	log.Info().Int64("data_length", gzr.BytesRead()).Int("line_count", gzr.LineCount()).Int("record_count", pw.RecordCount()).Dur("taken", time.Since(ts)).Msg("output")
}
