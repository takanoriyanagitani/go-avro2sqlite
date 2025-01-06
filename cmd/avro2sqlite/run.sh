#!/bin/sh

export ENV_SCHEMA_FILENAME=./sample.d/input.avsc

jsons2avro() {
	cat sample.d/sample.jsonl |
		json2avrows |
		cat >./sample.d/input.avro
}

#jsons2avro

export ENV_SQLITE_FILENAME=./sample.d/output.sqlite.db
export ENV_TRUSTED_SQL_FILENAME=./sample.d/trustedQuery.sql

test -f "${ENV_SQLITE_FILENAME}" &&
	rm "${ENV_SQLITE_FILENAME}"

which sqlite3 | fgrep -q sqlite3 || exec sh -c 'echo sqlite missing.; exit 1'

sqlite3 "${ENV_SQLITE_FILENAME}" < ./sample.d/create.sql

test -f "${ENV_TRUSTED_SQL_FILENAME}" ||
	exec \
		env name="${ENV_TRUSTED_SQL_FILENAME}" \
		sh -c 'echo query "(name=$name)" missing.; exit 1'

cat sample.d/input.avro |
	./avro2sqlite

sqlite3 \
	-markdown \
	"${ENV_SQLITE_FILENAME}" < ./sample.d/select.sql |
	mdcat
