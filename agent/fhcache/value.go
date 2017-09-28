// Copyright © 2017 Joyent, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fhcache

import (
	"os"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

// _Value is the FileHandleCache value.  _Value provides synchronization around
// opening and closing of file handles, however it is expected that the
type _Value struct {
	_Key

	// lock guards the remaining values.  The values in the Key
	// are immutable and therefore do not need to be guarded by a lock.  WTB
	// `const` modifier for compiler enforced immutability.  Where's my C++ when I
	// need it?
	lock *sync.RWMutex
	f    *os.File
}

func (fh *_Value) close() {
	fh.lock.Lock()
	defer fh.lock.Unlock()

	if err := fh.f.Close(); err != nil {
		log.Error().Err(err).Msg("unable to close FD")
	}
	fh.f = nil
	atomic.AddUint64(&closeFDCount, 1)
}

func (value *_Value) open(pgdataPath string) (*os.File, error) {
	filename := value._Key.filename(pgdataPath)
	f, err := os.Open(filename)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to open relation segment %q", filename)
	}
	atomic.AddUint64(&openFDCount, 1)

	return f, nil
}
