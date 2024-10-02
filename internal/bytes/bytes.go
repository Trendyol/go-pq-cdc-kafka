package bytes

import (
	"errors"
	"strconv"
	"strings"
)

type Size uint64

const (
	B  Size = 1
	KB Size = 1 << (10 * iota)
	MB
	GB
	TB
	PB
	EB
)

var unitMap = map[string]Size{
	"B":     B,
	"BYTE":  B,
	"BYTES": B,

	"KB":        KB,
	"KILOBYTE":  KB,
	"KILOBYTES": KB,

	"MB":        MB,
	"MEGABYTE":  MB,
	"MEGABYTES": MB,

	"GB":        GB,
	"GIGABYTE":  GB,
	"GIGABYTES": GB,

	"TB":        TB,
	"TERABYTE":  TB,
	"TERABYTES": TB,

	"PB":        PB,
	"PETABYTE":  PB,
	"PETABYTES": PB,

	"EB":       EB,
	"EXABYTE":  EB,
	"EXABYTES": EB,
}

var (
	ErrInvalidNumberFormat = errors.New("invalid number format")
	ErrUnknownUnit         = errors.New("unknown unit")
	ErrInvalidSizeFormat   = errors.New("invalid size format")
)

func ParseSize(sizeStr string) (Size, error) {
	sizeStr = strings.TrimSpace(sizeStr)

	for i, c := range sizeStr {
		if c < '0' || c > '9' {
			numberStr := sizeStr[:i]
			unitStr := sizeStr[i:]

			number, err := strconv.ParseFloat(numberStr, 64)
			if err != nil {
				return 0, ErrInvalidNumberFormat
			}

			unitStr = strings.ToUpper(strings.TrimSpace(unitStr))

			unitSize, ok := unitMap[unitStr]
			if !ok {
				return 0, ErrUnknownUnit
			}

			return Size(number) * unitSize, nil
		}
	}

	return 0, ErrInvalidSizeFormat
}
