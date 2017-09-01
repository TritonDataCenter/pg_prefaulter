package lsn

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

type LSN uint64

func Parse(in string) (LSN, error) {
	parts := strings.Split(in, "/")
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid LSN: %q", in)
	}

	id, err := strconv.ParseUint(parts[0], 16, 32)
	if err != nil {
		return 0, errors.Wrap(err, "unable to decode the segment ID")
	}

	offset, err := strconv.ParseUint(parts[1], 16, 32)
	if err != nil {
		return 0, errors.Wrap(err, "unable to decode the segment ID")
	}

	return LSN(uint64(id)<<32 | offset), nil
}

func (lsn LSN) ID() uint32 {
	return uint32(lsn >> 32)
}

func (lsn LSN) Offset() uint32 {
	return uint32(lsn)
}

func (lsn LSN) String() string {
	var id, offset uint32
	id = uint32(lsn >> 32)
	offset = uint32(lsn)
	return fmt.Sprintf("%X/%X", id, offset)
}
