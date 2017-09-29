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
	"fmt"
	"io/ioutil"
	"os/exec"
	"path"
	"regexp"
	"strconv"

	"github.com/joyent/pg_prefaulter/pg"
	"github.com/pkg/errors"
)

// 2>&1 pargs 80418 | grep 'startup process' | grep recovering
//
// argv[0]: postgres: startup process   recovering 00000001000002B8000000F9
var pargsRE = regexp.MustCompile(`^argv\[0\]: postgres: startup process[\s]+recovering[\s]+([0-9A-F]{24})`)

// FindWALFileFromPIDArgs searches a slice of PIDs to find the WAL filename
// being currently processed.
func FindWALFileFromPIDArgs(ctx context.Context, pids []PID) (walFilename pg.WALFilename, err error) {
	// Try getting the WAL Filename by sampling the PID args out of /proc.  If
	// this fails because the version of Illumos doesn't have this functionality,
	// proceed to trying to extract this information from pargs(1).  If that
	// fails, fall back to good 'ole ps(1).
	searchFuncs := []struct {
		name string
		fn   func(context.Context, []PID) (pg.WALFilename, error)
	}{
		{
			name: "/proc",
			fn:   findWALFileFromPIDArgsViaProc,
		},
		{
			name: "pargs(1)",
			fn:   findWALFileFromPIDArgsViaPArgs,
		},
		{
			name: "ps(1)",
			fn:   findWALFileFromPIDArgsViaPS,
		},
	}

	for _, pidSearch := range searchFuncs {
		walFilename, err = pidSearch.fn(ctx, pids)
		if err != nil {
			return "", errors.Wrapf(err, "unable to find arg from %s", pidSearch.name)
		}

		if walFilename != "" {
			return walFilename, nil
		}
	}

	return "", fmt.Errorf("unable to find a WAL filename")
}

func findWALFileFromPIDArgsViaPArgs(ctx context.Context, pids []PID) (pg.WALFilename, error) {
	// 2>&1 pargs `pgrep -P 80418` | grep 'startup process' | grep recovering
	//
	// argv[0]: postgres: startup process   recovering 00000001000002B8000000F9
	//
	// FIXME(seanc@): Perform an exec.LookPath() on startup to cache the absolute
	// path of pargs(1).
	var cmd *exec.Cmd
	{
		pidStrs := make([]string, len(pids))
		for n := range pids {
			pidStrs[n] = string(pids[n])
		}

		cmd = exec.CommandContext(ctx, "pargs", pidStrs...)
	}

	// pargs is rather noisy:
	//
	// pargs: Couldn't determine locale of target process.
	// pargs: Some strings may not be displayed properly.
	cmd.Stderr = ioutil.Discard

	pargsOut, err := cmd.Output()
	if err != nil {
		return "", errors.Wrap(err, "unable to exec pargs(1)")
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
		return "", errors.Wrap(err, "unable to extract PostgreSQL WAL segment from pargs(1)")
	}

	return pg.WALFilename(walSegment), nil
}

func findWALFileFromPIDArgsViaProc(ctx context.Context, pids []PID) (pg.WALFilename, error) {
	re := pargsRE.Copy()

	for _, pid := range pids {
		argvPath := path.Join("/proc", strconv.FormatInt(int64(pid), 10), "argv")
		argvOut, err := ioutil.ReadFile(argvPath)
		if err != nil {
			// Assume the PID terminated and continue processing
			continue
		}

		args := bytes.Split(argvOut, []byte("\x00"))
		// PostgreSQL's use of setproctitle(3) sets one large string with spaces.
		if len(args) != 1 {
			continue
		}

		md := re.Find(args[0])
		if md == nil {
			continue
		}

		walFilename := pg.WALFilename(md)
		if _, _, err := pg.ParseWalfile(walFilename); err == nil {
			return walFilename, nil
		}
	}

	return "", nil
}
