package dec

import (
	"bufio"
	"errors"
	"io"
	"iter"
	"os"

	ha "github.com/hamba/avro/v2"
	ho "github.com/hamba/avro/v2/ocf"

	as "github.com/takanoriyanagitani/go-avro2sqlite"
	. "github.com/takanoriyanagitani/go-avro2sqlite/util"
)

var (
	ErrInvalidSchema error = errors.New("invalid schema")
)

func ReaderToMapsHamba(
	rdr io.Reader,
	opts ...ho.DecoderFunc,
) iter.Seq2[map[string]any, error] {
	return func(yield func(map[string]any, error) bool) {
		buf := map[string]any{}
		var br io.Reader = bufio.NewReader(rdr)

		dec, e := ho.NewDecoder(br, opts...)
		if nil != e {
			yield(buf, e)
			return
		}

		for dec.HasNext() {
			clear(buf)

			e = dec.Decode(&buf)
			if !yield(buf, e) {
				return
			}
		}
	}
}

func ConfigToOpts(cfg as.DecodeConfig) []ho.DecoderFunc {
	var blobSizeMax int = cfg.BlobSizeMax

	var hcfg ha.Config
	hcfg.MaxByteSliceSize = blobSizeMax
	var hapi ha.API = hcfg.Freeze()

	return []ho.DecoderFunc{
		ho.WithDecoderConfig(hapi),
	}
}

func ReaderToMaps(
	rdr io.Reader,
	cfg as.DecodeConfig,
) iter.Seq2[map[string]any, error] {
	var opts []ho.DecoderFunc = ConfigToOpts(cfg)
	return ReaderToMapsHamba(
		rdr,
		opts...,
	)
}

func StdinToMaps(
	cfg as.DecodeConfig,
) iter.Seq2[map[string]any, error] {
	return ReaderToMaps(os.Stdin, cfg)
}

var StdinToMapsDefault IO[iter.Seq2[map[string]any, error]] = OfFn(
	func() iter.Seq2[map[string]any, error] {
		return StdinToMaps(as.DecodeConfigDefault)
	},
)

func FieldsToColumnNames(fields []*ha.Field) ([]string, error) {
	var ret []string = make([]string, 0, len(fields))
	for _, field := range fields {
		var name string = field.Name()
		ret = append(ret, name)
	}
	return ret, nil
}

func RecordSchemaToColumnNames(r *ha.RecordSchema) ([]string, error) {
	return FieldsToColumnNames(r.Fields())
}

func SchemaToColumnNamesHamba(s ha.Schema) ([]string, error) {
	switch t := s.(type) {
	case *ha.RecordSchema:
		return RecordSchemaToColumnNames(t)
	default:
		return nil, ErrInvalidSchema
	}
}

var SchemaToColumnNames func(schema string) ([]string, error) = ComposeErr(
	ha.Parse,
	SchemaToColumnNamesHamba,
)
