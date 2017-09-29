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

package proc

import (
	"bufio"
	"bytes"
	"context"
	"os/exec"
	"regexp"
	"strconv"
	"strings"

	"github.com/joyent/pg_prefaulter/pg"
	"github.com/pkg/errors"
)

type PID uint

// FindChildPIDs finds the child PIDs of a given process
func FindChildPIDs(ctx context.Context, pid PID) ([]PID, error) {
	pgrepOut, err := exec.CommandContext(ctx, "pgrep", "-P",
		strconv.FormatUint(uint64(pid), 10)).Output()
	if err != nil {
		if cErr, ok := err.(*exec.ExitError); ok {
			return nil, errors.Wrapf(err, "unable to exec pgrep(1): %+q", string(cErr.Stderr))
		} else {
			return nil, errors.Wrap(err, "unable to exec pgrep(1)")
		}
	}

	scanner := bufio.NewScanner(bytes.NewReader(pgrepOut))
	const defaultNumPids = 16
	pids := make([]PID, 0, defaultNumPids)
	for scanner.Scan() {
		pidStr := scanner.Text()

		pid64, err := strconv.ParseUint(pidStr, 10, 64)
		if err != nil {
			return nil, errors.Wrapf(err, "pgrep(1) returned non-integer argument: %+q", pidStr)
		}

		pids = append(pids, PID(pid64))
	}

	if err := scanner.Err(); err != nil {
		return nil, errors.Wrap(err, "unable to extract PostgreSQL PIDs")
	}

	return pids, nil
}

// $ ps -o command -p 13635,13636,13637,35959
//
// postgres: checkpointer process
// postgres: writer process
// postgres: wal writer process
// postgres: startup process   recovering 000000010000000C000000A1
var psRE = regexp.MustCompile(`^postgres: startup process[\s]+recovering[\s]+([0-9A-F]{24})`)

// findWALFileFromPIDArgsViaPS searches a slice of PIDs to find the WAL filename
// being currently processed by using the ps(1) command.
func findWALFileFromPIDArgsViaPS(ctx context.Context, pids []PID) (pg.WALFilename, error) {
	pidStr := make([]string, len(pids))
	for i, pid := range pids {
		pidStr[i] = strconv.FormatUint(uint64(pid), 10)
	}

	// FIXME(seanc@): Early on in the startup process, we should use
	// exec.LookPath("ps") and use the value found at process startup time.  And
	// if ps(1) can't be found because PATH isn't set, we should complain bitterly
	// and likely exit.
	psOut, err := exec.CommandContext(ctx, "ps", "-o", "command", "-p", strings.Join(pidStr, ",")).Output()
	if err != nil {
		return "", errors.Wrap(err, "unable to exec ps(1) args")
	}

	var walSegment string
	re := psRE.Copy()
	scanner := bufio.NewScanner(bytes.NewReader(psOut))
	for scanner.Scan() {
		line := scanner.Text()

		md := re.FindStringSubmatch(line)
		if md != nil && len(md) == 2 {
			walSegment = md[1]
			break
		}
	}

	if err := scanner.Err(); err != nil {
		return "", errors.Wrap(err, "unable to extract PostgreSQL WAL segment from ps(1) args")
	}

	return pg.WALFilename(walSegment), nil
}
