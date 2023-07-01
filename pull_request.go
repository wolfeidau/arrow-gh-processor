package main

import "github.com/apache/arrow/go/v13/arrow"

var (
	parquetMeta            = arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{"-1"})
	pullRequestArrowSchema = arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.BinaryTypes.String, Nullable: true, Metadata: parquetMeta},
			{Name: "type", Type: arrow.BinaryTypes.String, Nullable: true, Metadata: parquetMeta},
			{Name: "actor", Type: arrow.BinaryTypes.String, Nullable: true, Metadata: parquetMeta},
			{Name: "actor_url", Type: arrow.BinaryTypes.String, Nullable: true, Metadata: parquetMeta},
			{Name: "repo", Type: arrow.BinaryTypes.String, Nullable: true, Metadata: parquetMeta},
			{Name: "repo_url", Type: arrow.BinaryTypes.String, Nullable: true, Metadata: parquetMeta},
			{Name: "pull_action", Type: arrow.BinaryTypes.String, Nullable: true, Metadata: parquetMeta},
			{Name: "pull_number", Type: arrow.PrimitiveTypes.Int64, Nullable: true, Metadata: parquetMeta},
			{Name: "pull_state", Type: arrow.BinaryTypes.String, Nullable: true, Metadata: parquetMeta},
			{Name: "pull_title", Type: arrow.BinaryTypes.String, Nullable: true, Metadata: parquetMeta},
			{Name: "author_association", Type: arrow.BinaryTypes.String, Nullable: true, Metadata: parquetMeta},
			{Name: "created_at", Type: arrow.FixedWidthTypes.Timestamp_s, Nullable: true, Metadata: parquetMeta},
			{Name: "pull_request", Type: arrow.BinaryTypes.String, Nullable: true, Metadata: parquetMeta},
		}, nil)

	pullRequestJSONTemplate = `
{
"id": ${id},
"type":${type},
"actor":${actor.login},
"actor_url":${actor.url},
"repo":${repo.name},
"repo_url":${repo.url},
"pull_action":${payload.action},
"pull_number":${payload.number},
"pull_state":${payload.pull_request.state},
"pull_title":${payload.pull_request.title},
"pull_author_association":${payload.pull_request.author_association},
"created_at":${created_at},
"pull_request":${payload.pull_request;escape}
}`
)
