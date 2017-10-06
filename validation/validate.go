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
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

type ioTrace map[traceKey]traceValue

type traceKey struct {
	relation uint64
	segment  uint64
	offset   uint64
}

type traceValue struct {
	prefaulterCount uint64
	postgresCount   uint64
}

func main() {
	os.Exit(realMain())
}

func realMain() int {
	pfReader, pgReader, err := openTraces()
	if err != nil {
		log.Error().Err(err).Msg("unable to open prefaulter files")
		return 1
	}
	defer pfReader.Close()
	defer pgReader.Close()

	pfIO, err := initTraceMap(pfReader)
	if err != nil {
		log.Error().Err(err).Msg("unable to init prefaulter trace")
		return 2
	}

	// 3) Read the postgres map to verify that every IO performed by the
	//    prefaulter was performed by PostgreSQL
	if err := compareIO(pfIO, pgReader); err != nil {
		log.Error().Err(err).Msg("unable to compare prefaulter trace")
		return 3
	}

	// 4)

	return 0
}

func openTraces() (prefaulterFile, postgresFile *os.File, err error) {
	filename := "iotrace.pg_prefaulter"
	pfReader, err := os.Open(filename)
	if err != nil {
		log.Error().Err(err).Str("filename", filename).Msg("unable to open prefaulter trace")
		return nil, nil, errors.Wrapf(err, "unable to open %q", filename)
	}

	filename = "iotrace.postgres"
	pgReader, err := os.Open(filename)
	if err != nil {
		log.Error().Err(err).Str("filename", filename).Msg("unable to open postgres trace")
		return nil, nil, errors.Wrapf(err, "unable to open %q", filename)
	}

	return pfReader, pgReader, nil
}

func initTraceMap(f *os.File) (ioMap ioTrace, err error) {
	// 1) Read the prefaulter first because it is smaller in size
	const defaultTraceSize = 4096
	pfIO := make(ioTrace, defaultTraceSize)

	// 2) Populate the prefaulter IO map
	scanner := bufio.NewScanner(f)
	var lineNo uint64 = 0
	for scanner.Scan() {
		line := scanner.Text()
		lineNo++

		in, err := parseLine(line)
		switch {
		case in == traceKey{} && err == nil:
			continue
		case err != nil:
			log.Error().Uint64("line-number", lineNo).Err(err).Str("line", line).Msg("error parsing line")
			continue
		}

		if v, found := pfIO[in]; found {
			v.prefaulterCount++
			pfIO[in] = v
		} else {
			pfIO[in] = traceValue{
				prefaulterCount: 1,
			}
		}
	}

	if err := scanner.Err(); err != nil {
		log.Error().Err(err).Msg("error scanning initial input")
		return nil, errors.Wrap(err, "error scanning initial input")
	}

	return pfIO, nil
}

func compareIO(expect ioTrace, r *os.File) error {
	scanner := bufio.NewScanner(r)
	var lineNo uint64 = 0
	for scanner.Scan() {
		line := scanner.Text()
		lineNo++
		in, err := parseLine(line)
		if err != nil {
			log.Error().Uint64("line-number", lineNo).Err(err).Str("line", line).Msg("error parsing line")
			continue
		}

		if v, found := expect[in]; found {
			v.postgresCount++
			expect[in] = v
		} else {
			expect[in] = traceValue{
				postgresCount: 1,
			}
		}
	}

	if err := scanner.Err(); err != nil {
		log.Error().Err(err).Msg("error scanning comparison input")
		return errors.Wrap(err, "error scanning comparison input")
	}

	// Iterate over the input to make sure that every IO found in PG was found in
	var statUnusedPrefaultedPage, statPrefaultedPage uint64
	for k, v := range expect {
		statPrefaultedPage++
		if v.prefaulterCount > 0 && v.postgresCount == 0 {
			statUnusedPrefaultedPage++
			log.Debug().
				Uint64("relation", k.relation).
				Uint64("segment", k.segment).
				Uint64("offset", k.offset).
				Msg("prefaulter faulted a page that PostgreSQL never read in")
		}
	}

	log.Info().
		Uint64("prefaulted-page", statPrefaultedPage).
		Uint64("prefaulted-unused-page", statUnusedPrefaultedPage).
		Str("pct-hit-rate", fmt.Sprintf("%0.3f%%", float64(statUnusedPrefaultedPage)/float64(statPrefaultedPage)*100.0)).
		Msg("unprefaulter faulted a page that PostgreSQL never read in (or hadn't read in yet)")

	return nil
}

func parseLine(line string) (in traceKey, err error) {
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return traceKey{}, nil
	}

	relationStr := strings.SplitN(fields[3], ".", 2)

	// Skip uninteresting IO operations
	{
		switch relationStr[0] {
		case
			"PG_VERSION",
			"global",
			"pg_control",
			"pg_filenode",
			"pg_internal",
			"pg_xlogdump",
			"postmaster",
			"xlogtemp":
			return traceKey{}, nil
		}

		if strings.HasSuffix(relationStr[0], "_fsm") ||
			strings.HasSuffix(relationStr[0], "_vm") ||
			strings.HasPrefix(relationStr[0], "db_") {
			return traceKey{}, nil
		}
	}

	in.relation, err = strconv.ParseUint(relationStr[0], 10, 64)
	if err != nil {
		log.Error().Err(err).Str("line", line).Msg("error parsing relation")
		return traceKey{}, err
	}

	if len(relationStr) > 1 {
		in.segment, err = strconv.ParseUint(relationStr[1], 10, 64)
		if err != nil {
			log.Error().Err(err).Str("line", line).Msg("error parsing segment")
			return traceKey{}, err
		}
	}

	offStr := strings.SplitN(fields[4], "=", 2)
	if len(offStr) != 2 {
		log.Error().Err(err).Str("line", line).Msg("error tokenizing offset")
		return traceKey{}, err
	}
	in.offset, err = strconv.ParseUint(offStr[1], 10, 64)
	if err != nil {
		log.Error().Err(err).Str("line", line).Msg("error parsing offset")
		return traceKey{}, err
	}

	return in, nil
}
