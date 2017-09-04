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

package cmd

import (
	"fmt"

	"github.com/joyent/pg_prefaulter/buildtime"
	"github.com/spf13/cobra"
)

// versionCmd displays version information
var versionCmd = &cobra.Command{
	Use:   "version",
	Short: buildtime.PROGNAME + ` version information`,
	Long:  fmt.Sprintf(`Display %s version information`, buildtime.PROGNAME),

	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Printf("%s:\n", buildtime.PROGNAME)
		fmt.Printf("\tversion: %s\n", buildtime.VERSION)
		fmt.Printf("\tcommit: %s\n", buildtime.COMMIT)
		fmt.Printf("\tdate: %s\n", buildtime.DATE)
		fmt.Printf("\ttag: %s\n", buildtime.TAG)
		return nil
	},
}

func init() {
	RootCmd.AddCommand(versionCmd)
}
