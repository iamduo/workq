# Captain's Log

**Captain's Log is internal to Workq for the time being. It is meant to be an external package at a later time after it has been vetted. This README is  incomplete for this reason.**

Captain's log is an write ahead data log with a small API for Go.

It is useful for projects that require simple operational persistence to log files for recovery, essentially a replay log.

## Features

* Segment support - Split after a minimum file size.
* CRC32 support - CRC checksum.
* User defined cleaning strategy.

## Getting Started

## Stream

The Stream represents a single data stream contained within a log directory and split into multiple segment files.
A stream can be [appended](#appender) to, [iterated](#cursor), and [cleaned](#cleaning).

A Stream object can be created using the `NewStream` function and requires a path
to the log directory and the magic header which contains the magic number and version. The header is written to every new segment file and is verified on every segment file when iterating through the data stream.

```go
// 0x6370746e -> "cptn"
stream := NewStream(path, &MagicHeader{Magic: 0x6370746e, Version: 1})
```

### Appender

An Appender writes into the data stream in append-only mode. The data stream is split into segment files within the log directory. The split is specified by the `SegmentSize` option which represents the minimum file size to be reached before splitting into a new file.

The primary benefit to split is to simultaneously allow the [cleaning](#cleaning) of log records while appending.

#### Opening an Appender

An Appender can be opened using the `OpenAppender` method on the Stream object:

```go
appender, err := stream.OpenAppender(&AppendOptions{
  // SegmentSize: 67108864, // Default 64MiB in bytes
  // SyncPolicy: captain.SyncOS, // Default
  // SyncInterval: 1000, // Sync interval in ms, used when SyncPolicy is set to "captain.SyncInterval"
})
if err != nil {
  // ...
}
```

#### Options

A stream can be opened with these additional options passed in during the OpenAppender call:

* `Options.SegmentSize` - The minimum size of a segment file before creating a new one. Defaults to 64 MiB.
* `Options.SyncPolicy` - Disk sync policy to use:
  * `captain.SyncOS` - (Default) Sync deferred to the operating system.
  * `captain.SyncAlways` - Sync after every append. This will be relatively slow.
  * `captain.SyncInterval` Sync at a specified interval in milliseconds set by the `Options.SyncInterval`.
* `Options.SyncInterval` - Sync interval in milliseconds. Used when `Options.SyncPolicy` is set to `captain.SyncInterval`.

#### Appending Data

A data stream can only have a single appender at a time and must be locked before use:

```go
if err := appender.Lock(); err != nil {
  // ...
}
defer appender.Unlock()

err := appender.Append([]byte{})
if err != nil {
  // ...
}
```

An Appender lock is across processes (via advisory lock) and must be released when finished. Locking an Appender for a stream that already has a locked Appender will block indefinitely until released by the first Appender. Appenders do not block cursors or cleaners.

### Cursor

A data stream can be iterated using a Cursor. A cursor can be acquired by the `OpenCursor` method on the Stream object:

```go
cursor, err := stream.OpenCursor()
if err != nil {
  // ...
}
```

The `Next()` method returns the next log record in the data stream as a `Record` object along with an error (*Record, error). The Record object contains the log **Time** and the original data as the **Payload**. `Next()` can return an error if the record could not be fully read or the CRC checksum did not match. When there are no more records remaining, `nil, nil` will be returned signifying the the graceful end of the stream.

For a consistent view of the stream data[1], a read lock through `Lock()` is required before iteration and must be released through `Unlock()` when finished:

```go
if err := cursor.Lock(); err != nil {
  // ...
}
defer cursor.Unlock()

for {
  rec, err := cursor.Next()
  if err != nil {
    // ... handle err
  }

  if rec == nil {
    // End of stream
    break
  }

  fmt.Printf("Time=%s, Payload=%s", r.Time, r.Payload)
}
```

[1] Cleaners can rewrite segment files and acquires an exclusive lock blocking cursors.

### Cleaning

Cleaning is the process of removing old log records that are considered stale and requires a user defined "clean" function. A clean function determines if a log record should be permanently removed. The clean function is invoked with the path of the log segment file and a Record object on every log entry. The clean function should return true to signal to the cleaner to delete or false to retain. If an error is returned, processing stops. The sole exception is when ErrSkipSegment is returned
to signal to skip the current segment entirely.

A Cleaner will only visit past read-only segmented files and not the most current one being appended to until it becomes read-only. Keeping segment-size reasonably sized will allow the cleaner to work efficiently.

A cleaner can be opened using the `OpenCleaner` method on the Stream object:

```go
cleaner, err := stream.OpenCleaner()
if err != nil {
  // ...
}
```

Cleaning is invoked by the `Clean` method and passing in the cleaning function. The cleaning process must be locked through `Cleaner.Lock()` and released with `Cleaner.Unlock()` when finished. Cleaning locks only readers and not appenders as it only acts on read-only segment files.

```go
if err := cleaner.Lock(); err != nil {
  // ...
}
defer cleaner.Unlock()

clean := func(path string, r *Record) (bool, error) {
  // Clean all records over 24 hours old.
  age := time.Now().Sub(r.Time)
  if age >= time.Duration(24) * time.Hour {
    return true, nil
  }

  return false, nil
}
err := cleaner.Clean(clean)
if err != nil {
  // ...
}
```

Cleaning works by rewriting the segment file with only the relevant log records.

## Implementation

### File Names

Captain's log files are segmented into sequentially named log files.

Sequential log filenames are 9 digit, left padded with 0s and have a "log" extension:

```text
Format: {SEQUENCE}.log
Example: 000000001.log
```

Sequence number and follows natural order based on write time. It is rotated after the minimum segment size has been reached.

### File Format

Log files start with a magic header and contain a sequence of variable size records.

```text
|MAGIC_HEADER|RECORD|RECORD|RECORD|...
```

#### Magic header

Every log file starts with the specified magic header in the Stream object (8 bytes). The first four bytes translates to the magic number and the last 4 bytes is the specified version as a uint32 in big endian.

#### Record Format

```text
Record
|TIME|SIZE|PAYLOAD|CRC|
```

* TIME (15 Bytes) - Time in UTC RFC 3399 with nanoseconds.
  * byte 0: version
  * bytes 1-8: seconds
  * bytes 9-12: nanoseconds
  * bytes 13-14: zone offset in minutes (reserved for future use, always 0 for UTC)
* SIZE (Varint) - Length of payload
* PAYLOAD - Byte stream with the length of SIZE.
* CRC (4 Bytes) - 32 bit hash computed on TIME,SIZE,PAYLOAD
