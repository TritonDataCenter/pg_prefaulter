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
	"path"
	"strconv"

	"github.com/joyent/pg_prefaulter/config"
	"github.com/joyent/pg_prefaulter/lsn"
	"github.com/pkg/errors"
	log "github.com/rs/zerolog/log"
	"github.com/spf13/viper"
)

// Filename returns the filename of a given relation
func (rf *_IOCacheValue) Filename() (string, error) {
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
func (rf *_IOCacheKey) PageNum() (int64, error) {
	blockNumber, err := strconv.ParseUint(rf.Block, 10, 64)
	if err != nil {
		log.Warn().Err(err).Str("str int", rf.Block).Msgf("invalid integer: %+v", rf)
		return -1, errors.Wrapf(err, "unable to parse block number")
	}

	pageNum := int64(blockNumber) % int64(lsn.MaxSegmentSize/lsn.WALPageSize)

	return pageNum, nil
}

// PageNum returns the pagenum of a given relation
func (rf *_IOCacheValue) PageNum() (int64, error) {
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

func (rf *_IOCacheValue) populateSelf() error {
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
