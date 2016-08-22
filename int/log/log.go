package log

import (
	"log"
	"os"
)

var OLogger = log.New(os.Stdout, "", log.LstdFlags)
var DLogger = OLogger
var ELogger = log.New(os.Stderr, "", log.LstdFlags)
