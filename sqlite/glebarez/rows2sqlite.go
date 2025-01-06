package rows2sqlite

import (
	"context"
	"iter"

	_ "github.com/glebarez/go-sqlite"

	. "github.com/takanoriyanagitani/go-avro2sqlite/util"

	s2 "github.com/takanoriyanagitani/go-avro2sqlite/sqlite"
)

func RowsToSqliteFile(
	ctx context.Context,
	rows iter.Seq2[[]any, error],
	filename string,
	trustedQuery string,
) error {
	return s2.RowsToSqliteFile(
		ctx,
		rows,
		"sqlite",
		filename,
		trustedQuery,
	)
}

func FilenameToQueryToRowSaver(
	filename string,
) func(trustedQuery string) s2.RowSaver {
	return func(trustedQuery string) s2.RowSaver {
		return func(i iter.Seq2[[]any, error]) IO[Void] {
			return func(ctx context.Context) (Void, error) {
				return Empty, RowsToSqliteFile(
					ctx,
					i,
					filename,
					trustedQuery,
				)
			}
		}
	}
}
