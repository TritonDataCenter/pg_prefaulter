package fhcache

import (
	"testing"

	"github.com/kylelemons/godebug/pretty"
)

func Test_Key_filename(t *testing.T) {
	tests := []struct {
		key      _Key
		path     string
		filename string
	}{
		{
			key: _Key{
				tablespace: 1663,
				database:   16398,
				relation:   24576,
				segment:    0,
			},
			path:     "/test/path/pgdata",
			filename: "/test/path/pgdata/base/16398/24576",
		},
	}

	for n, test := range tests {
		if diff := pretty.Compare(test.key.filename(test.path), test.filename); diff != "" {
			t.Fatalf("%d: filename diff: (-got +want)\n%s", n, diff)
		}
	}
}
