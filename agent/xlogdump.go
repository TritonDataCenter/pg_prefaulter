package agent

import (
	"bufio"
	"bytes"
	"os"
	"os/exec"
	"path"
	"regexp"

	"github.com/bluele/gcache"
	"github.com/joyent/pg_prefaulter/config"
	"github.com/pkg/errors"
	log "github.com/rs/zerolog/log"
	"github.com/spf13/viper"
)

// Input to parse: rel 1663/16394/1249 blk 29
//                     ^^^^ ------------------- Tablespace ID
//                          ^^^^^ ------------- Database ID
//                                ^^^^ -------- Relation ID
//                                         ^^ - Block Number
var pgXLogDumpRE = regexp.MustCompile(`rel ([\d]+)/([\d]+)/([\d]+) blk ([\d]+)`)

// https://github.com/snaga/xlogdump
//
// [cur:CC/DFFF7C8, xid:448891062, rmid:11(Btree), len/tot_len:66/98, info:0, prev:C3/4FFF758] insert_leaf: s/d/r:1663/16385/16442 tid 1317010/91
// [cur:C4/70, xid:450806558, rmid:10(Heap), len/tot_len:737/769, info:0, prev:C4/20] insert: s/d/r:1663/16385/16431 blk/off:32400985/3 header: t_infomask2 12 t_infomask 2051 t_hoff 32
var xlogdumpRE = regexp.MustCompile(`s/d/r:([\d]+)/([\d]+)/([\d]+) (?:tid |blk/off:)([\d]+)`)

var (
	xlogRE *regexp.Regexp
)

// prefaultWALFile shells out to pg_xlogdump(1) and reads its input.  The input
// from pg_xlogdump(1) is then turned into IO requests that are picked up and
// handled by the ioCache.
func (a *Agent) prefaultWALFile(walFile string) error {
	re := xlogRE.Copy()
	pgdataPath := viper.GetString(config.KeyPGData)
	var linesMatched, linesScanned, walFilesProcessed uint64

	fileName := path.Join(pgdataPath, "pg_xlog", walFile)

	_, err := os.Stat(fileName)
	if err != nil {
		log.Debug().Err(err).Msg("stat")
		return errors.Wrap(err, "WAL file does not exist")
	}

	cmd := exec.CommandContext(a.shutdownCtx, viper.GetString(config.KeyXLogPath), fileName)
	stdoutStderr, err := cmd.CombinedOutput()
	if err != nil {
		log.Debug().Err(err).Msg("exec")
		return errors.Wrapf(err, "unable to run %q", viper.GetString(config.KeyXLogPath))
	}

	if len(stdoutStderr) == 0 {
		log.Warn().Str("out", string(stdoutStderr)).Msg("unable to process WAL file")
		log.Debug().Msg("nada")
		return nil
	}

	scanner := bufio.NewScanner(bytes.NewReader(stdoutStderr))
	for scanner.Scan() {
		line := scanner.Bytes()
		linesScanned++
		submatches := re.FindAllSubmatch(line, -1)
		if submatches != nil {
			linesMatched++
		}
		for _, matches := range submatches {
			_, err := a.ioReqCache.GetIFPresent(_RelationFile{
				Tablespace: string(matches[1]),
				Database:   string(matches[2]),
				Relation:   string(matches[3]),
				Block:      string(matches[4]),
			})
			if err == gcache.KeyNotFoundError {
				// cache miss
			}
		}
	}
	if err := scanner.Err(); err != nil {
		log.Warn().Err(err).Msg("scanning output")
	}
	walFilesProcessed++

	a.metrics.Add(metricsXLogPrefaulted, walFilesProcessed)
	a.metrics.RecordValue(metricsXLogDumpLen, float64(len(stdoutStderr)))
	a.metrics.RecordValue(metricsXLogDumpLinesMatched, float64(linesMatched))
	a.metrics.RecordValue(metricsXLogDumpLinesScanned, float64(linesScanned))

	return nil
}
