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

package agent

import (
	"fmt"
	"os"
	"path"
	"strconv"
	"sync"

	"github.com/joyent/pg_prefaulter/config"
	"github.com/joyent/pg_prefaulter/lsn"
	"github.com/pkg/errors"
	log "github.com/rs/zerolog/log"
	"github.com/spf13/viper"
)

// _RelationFile contains the forward lookup information for a given relation
// file.  _RelationFile is a
// [comparable](https://golang.org/ref/spec#Comparison_operators) struct used as
// a lookup key.  These values are immutable and map 1:1 with the string inputs
// read from the xlog scanning utility.
type _RelationFile struct {
	Tablespace string
	Database   string
	Relation   string
	Block      string

	// memoized values
	lock     sync.Mutex
	filename string
	pageNum  int64
}

// Open calculates the relation filename and returns an open file handle.
func (rf *_RelationFile) Open() (*os.File, error) {
	filename, err := rf.Filename()
	if err != nil {
		log.Warn().Err(err).Msgf("unable to determine filename of relation %+v", rf)
		return nil, errors.Wrapf(err, "unable to determine filename %+v", rf)
	}

	f, err := os.Open(filename)
	if err != nil {
		log.Warn().Err(err).Msgf("unable to open relation name %q", filename)
		return nil, errors.Wrapf(err, "unable to open relation name %q", filename)
	}

	return f, nil
}

// Filename returns the filename of a given relation
func (rf *_RelationFile) Filename() (string, error) {
	rf.lock.Lock()
	defer rf.lock.Unlock()
	if rf.filename == "" {
		if err := rf.populateSelf(); err != nil {
			log.Warn().Err(err).Msg("populating self")
			return "", errors.Wrap(err, "populating self")
		}
	}

	return rf.filename, nil
}

// PageNum returns the pagenum of a given relation
func (rf *_RelationFile) PageNum() (int64, error) {
	rf.lock.Lock()
	defer rf.lock.Unlock()
	if rf.filename == "" {
		if err := rf.populateSelf(); err != nil {
			log.Warn().Err(err).Msg("populating self")
			return -1, errors.Wrap(err, "populating self")
		}
	}

	return rf.pageNum, nil
}

func (rf *_RelationFile) populateSelf() error {
	blockNumber, err := strconv.ParseUint(rf.Block, 10, 64)
	if err != nil {
		log.Warn().Err(err).Str("str int", rf.Block).Msgf("invalid integer: %+v", rf)
		return errors.Wrapf(err, "unable to parse block number")
	}

	rf.pageNum = int64(blockNumber) % int64(lsn.MaxSegmentSize/lsn.WALPageSize)
	fileNum := int64(blockNumber) / int64(lsn.MaxSegmentSize/lsn.WALPageSize)
	filename := rf.Relation
	if fileNum > 0 {
		// It's easier to abuse Relation here than to support a parallel refilno
		// struct member
		filename = fmt.Sprintf("%s.%d", rf.Relation, fileNum)
	}

	rf.filename = path.Join(viper.GetString(config.KeyPGData), "base", string(rf.Database), string(filename))

	return nil
}
