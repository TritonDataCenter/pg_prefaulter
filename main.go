// Copyright © 2017 Sean Chittenden <sean@chittenden.org>
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

package main

import (
	"github.com/joyent/pg_prefaulter/buildtime"
	"github.com/joyent/pg_prefaulter/cmd"
)

func main() {
	cmd.Execute()
}

// A collection of variables set at link-time
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
	tag     = ""
)

// Foist various build-time constants into the buildtime package so they can be
// referenced from different packages.
func init() {
	buildtime.COMMIT = commit
	buildtime.VERSION = version
	buildtime.DATE = date
	buildtime.TAG = tag
}
