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

import "github.com/pkg/errors"

// runPrimary is executed when talking to a writable database.
func (a *Agent) runPrimary() (loopImmediately bool, err error) {
	// Connect to the primary and see what the lag is in bytes between the primary
	// and its connected followers.  Report out a histogram of lag.

	if _, err = a.queryLag(_QueryLagPrimary); err != nil {
		return false, errors.Wrap(err, "unable to query primary lag")
	}

	if _, err = a.queryLastLog(); err != nil {
		return false, errors.Wrap(err, "unable to query last WAL lag")
	}

	return false, nil
}
