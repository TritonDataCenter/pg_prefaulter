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

// +build solaris

package proc

import (
	"bufio"
	"bytes"
	"context"
	"io/ioutil"
	"os/exec"
	"regexp"

	"github.com/joyent/pg_prefaulter/pg"
	"github.com/pkg/errors"
)

// 2>&1 pargs 80418 | grep 'startup process' | grep recovering
//
// argv[0]: postgres: startup process   recovering 00000001000002B8000000F9
var pargsRE = regexp.MustCompile(`^argv\[0\]: postgres: startup process[\s]+recovering[\s]+([0-9A-F]{24})`)

// FindWALFileFromPIDArgs searches a slice of PIDs to find the WAL filename
// being currently processed.
func FindWALFileFromPIDArgs(ctx context.Context, pids []PID) (pg.WALFilename, error) {
	// 2>&1 pargs `pgrep -P 80418` | grep 'startup process' | grep recovering
	//
	// argv[0]: postgres: startup process   recovering 00000001000002B8000000F9
	cmd := exec.CommandContext(ctx, "pargs", pids...)

	// pargs is rather noisy:
	//
	// pargs: Couldn't determine locale of target process.
	// pargs: Some strings may not be displayed properly.
	cmd.Stderr = ioutil.Discard

	pargsOut, err := cmd.Output()
	if err != nil {
		return nil, errors.Wrap(err, "unable to exec pargs(1)")
	}

	var walSegment string
	re := pargsRE.Copy()
	scanner := bufio.NewScanner(bytes.NewReader(pargsOut))
	for scanner.Scan() {
		line := scanner.Text()

		walSegment = re.FindString(line)
		if walSegment != "" {
			break
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, errors.Wrap(err, "unable to extract PostgreSQL WAL segment from pargs(1)")
	}

	return pgWALFilename(walSegment), nil
}
