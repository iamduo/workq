// +build gofuzz

package prot

import (
	"bufio"
	"bytes"
)

func FuzzCmd(data []byte) int {
	cmd, err := ParseCmd(bufio.NewReader(bytes.NewReader(data)))
	if err != nil {
		if cmd != nil {
			panic("cmd != nil")
		}
		return 0
	}

	return 1
}
