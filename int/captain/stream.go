package captain

// Stream represents a data stream.
// Can be read in sequence using a Cursor.
// Can be appended to using an Appender.
// Can be cleaned using a Cleaner.
type Stream struct {
	path   string       // Path to the data log directory.
	header *MagicHeader // Header to use in segment files.
}

// NewStream returns a Stream with a specified path and header to use.
func NewStream(path string, header *MagicHeader) *Stream {
	return &Stream{
		path:   path,
		header: header,
	}
}
