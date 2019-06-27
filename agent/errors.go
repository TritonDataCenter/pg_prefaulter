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

import "fmt"

type purgeEventError interface {
	purgeCache() bool
}

type retryableError interface {
	retry() bool
}

type walError struct {
	_err        error
	_retry      bool
	_purgeCache bool
}

type versionError struct {
	_err    error
	_retry  bool
}

func newWALError(err error, retry bool, purge bool) walError {
	return walError{
		_err:        err,
		_retry:      retry,
		_purgeCache: purge,
	}
}

func newVersionError(err error, retry bool) versionError {
	return versionError{
		_err:    err,
		_retry:  retry,
	}
}

// Error returns the error message
func (walErr walError) Error() string {
	return fmt.Sprintf("%v: (retriable: %t, purge cache: %t", walErr._err, walErr._retry, walErr._purgeCache)
}

func (walErr walError) retry() bool {
	return walErr._retry
}

func (walErr walError) purgeCache() bool {
	return walErr._purgeCache
}

func (versionErr versionError) Error() string {
	return fmt.Sprintf("%v (retriable: %t)", versionErr._err, versionErr._retry)
}

func (versionErr versionError) retry() bool {
	return versionErr._retry
}
