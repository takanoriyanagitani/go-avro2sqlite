package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"iter"
	"log"

	. "github.com/takanoriyanagitani/go-avro2sqlite/util"
)

type RowSaver func(iter.Seq2[[]any, error]) IO[Void]

func RowsToTx(
	ctx context.Context,
	rows iter.Seq2[[]any, error],
	tx *sql.Tx,
	trustedQuery string,
) error {
	stmt, e := tx.PrepareContext(ctx, trustedQuery)
	if nil != e {
		return e
	}
	defer stmt.Close()

	for row, e := range rows {
		if nil != e {
			return e
		}

		_, e := stmt.ExecContext(ctx, row...)
		if nil != e {
			return e
		}
	}

	return nil
}

func RowsToDb(
	ctx context.Context,
	rows iter.Seq2[[]any, error],
	db *sql.DB,
	trustedQuery string,
) error {
	tx, e := db.BeginTx(ctx, nil)
	if nil != e {
		return e
	}
	defer func() {
		e := tx.Rollback()
		if nil != e {
			if !errors.Is(e, sql.ErrTxDone) {
				log.Printf("error on rollback: %v\n", e)
			}
		}
	}()

	e = RowsToTx(ctx, rows, tx, trustedQuery)
	if nil != e {
		return e
	}
	return tx.Commit()
}

func RowsToSqlite(
	ctx context.Context,
	rows iter.Seq2[[]any, error],
	driverName string,
	dsn string,
	trustedQuery string,
) error {
	db, e := sql.Open(driverName, dsn)
	if nil != e {
		return e
	}
	defer db.Close()
	return RowsToDb(ctx, rows, db, trustedQuery)
}

func RowsToSqliteFile(
	ctx context.Context,
	rows iter.Seq2[[]any, error],
	driverName string,
	filename string,
	trustedQuery string,
) error {
	return RowsToSqlite(ctx, rows, driverName, filename, trustedQuery)
}

type MapToArray func(map[string]any) IO[[]any]

type ColumnNames []string

func (c ColumnNames) ToMapToArray() MapToArray {
	var buf []any
	return func(m map[string]any) IO[[]any] {
		return func(_ context.Context) ([]any, error) {
			buf = buf[:0]

			for _, colname := range c {
				var val any = m[colname]
				buf = append(buf, val)
			}
			return buf, nil
		}
	}
}

func (m MapToArray) MapsToRows(
	rows iter.Seq2[map[string]any, error],
) IO[iter.Seq2[[]any, error]] {
	return func(ctx context.Context) (iter.Seq2[[]any, error], error) {
		return func(
			yield func([]any, error) bool,
		) {
			for row, e := range rows {
				if nil != e {
					yield(nil, e)
					return
				}

				arr, e := m(row)(ctx)
				if !yield(arr, e) {
					return
				}
			}
		}, nil
	}
}
