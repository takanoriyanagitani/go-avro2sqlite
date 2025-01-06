package main

import (
	"context"
	"fmt"
	"io"
	"iter"
	"log"
	"os"
	"strconv"
	"strings"

	. "github.com/takanoriyanagitani/go-avro2sqlite/util"

	s2 "github.com/takanoriyanagitani/go-avro2sqlite/sqlite"
	sg "github.com/takanoriyanagitani/go-avro2sqlite/sqlite/glebarez"

	dh "github.com/takanoriyanagitani/go-avro2sqlite/avro/dec/hamba"
)

var EnvValByKey func(string) IO[string] = Lift(
	func(key string) (string, error) {
		val, found := os.LookupEnv(key)
		switch found {
		case true:
			return val, nil
		default:
			return "", fmt.Errorf("env var %s missing", key)
		}
	},
)

var stdin2maps IO[iter.Seq2[map[string]any, error]] = dh.
	StdinToMapsDefault

var schemaFilename IO[string] = EnvValByKey("ENV_SCHEMA_FILENAME")

const SchemaFileSizeMaxDefault int64 = 1048576

var schemaContent IO[string] = Bind(
	schemaFilename,
	FilenameToStringLimited(SchemaFileSizeMaxDefault),
)

var columnNames IO[[]string] = Bind(
	schemaContent,
	Lift(dh.SchemaToColumnNames),
)

var map2arr IO[s2.MapToArray] = Bind(
	columnNames,
	Lift(func(c []string) (s2.MapToArray, error) {
		return s2.ColumnNames(c).ToMapToArray(), nil
	}),
)

var mapdRows IO[iter.Seq2[[]any, error]] = Bind(
	map2arr,
	func(m s2.MapToArray) IO[iter.Seq2[[]any, error]] {
		return Bind(
			stdin2maps,
			m.MapsToRows,
		)
	},
)

var sqliteOutputName IO[string] = EnvValByKey("ENV_SQLITE_FILENAME")
var trustedSqlFilename IO[string] = EnvValByKey("ENV_TRUSTED_SQL_FILENAME")

var trustedQueryLengthMax IO[int] = Bind(
	EnvValByKey("ENV_TRUSTED_SQL_LEN"),
	Lift(strconv.Atoi),
).Or(Of(1048576))

func FilenameToStringLimited(limit int64) func(string) IO[string] {
	return Lift(func(filename string) (string, error) {
		f, e := os.Open(filename)
		if nil != e {
			return "", e
		}
		defer f.Close()

		limited := &io.LimitedReader{
			R: f,
			N: limit,
		}

		var buf strings.Builder
		_, e = io.Copy(&buf, limited)
		return buf.String(), e
	})
}

var trustedQueryContent IO[string] = Bind(
	trustedQueryLengthMax,
	func(i int) IO[string] {
		return Bind(
			trustedSqlFilename,
			FilenameToStringLimited(int64(i)),
		)
	},
)

type OutCfg struct {
	SqliteOutputName string
	TrustedQuery     string
}

var outcfg IO[OutCfg] = Bind(
	All(
		sqliteOutputName,
		trustedQueryContent,
	),
	Lift(func(s []string) (OutCfg, error) {
		return OutCfg{
			SqliteOutputName: s[0],
			TrustedQuery:     s[1],
		}, nil
	}),
)

var rowSaver IO[s2.RowSaver] = Bind(
	outcfg,
	Lift(func(cfg OutCfg) (s2.RowSaver, error) {
		return sg.FilenameToQueryToRowSaver(
			cfg.SqliteOutputName,
		)(cfg.TrustedQuery), nil
	}),
)

var rows2sqlite IO[Void] = Bind(
	rowSaver,
	func(s s2.RowSaver) IO[Void] {
		return Bind(
			mapdRows,
			s,
		)
	},
)

var sub IO[Void] = func(ctx context.Context) (Void, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	return rows2sqlite(ctx)
}

func main() {
	_, e := sub(context.Background())
	if nil != e {
		log.Printf("%v\n", e)
	}
}
