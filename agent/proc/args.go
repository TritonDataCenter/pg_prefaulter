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
	"strconv"

	"github.com/pkg/errors"
)

type PID uint

const (
	MetricsWALLookupMode = "agent-wal-lookup-mode"
)

// FindChildPIDs finds the child PIDs of a given process
func FindChildPIDs(ctx context.Context, pid PID) ([]PID, error) {
	// FIXME(seanc@): The call to exec.LookPath("pgrep") should probably be
	// performed at process startup and cached.
	pgrepPath, err := exec.LookPath("pgrep")
	if err != nil {
		return nil, errors.Wrap(err, "unable to find pgrep(1)")
	}

	pgrepOut, err := exec.CommandContext(ctx, pgrepPath, "-P",
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
