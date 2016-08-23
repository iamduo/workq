# Workq [![Build Status](https://travis-ci.org/iamduo/workq.svg?branch=master)](https://travis-ci.org/iamduo/workq) ![Project Status](https://img.shields.io/badge/status-alpha-yellow.svg)

Workq is a job scheduling server strictly focused on simplifying job processing and streamlining coordination. It can run jobs in blocking foreground or non-blocking background mode.

Workq runs as a standalone TCP server and implements a simple, text based protocol. Clients interact with Workq over a TCP socket in a request/response model with text [commands](doc/protocol.md#client-commands). Please refer to the [full protocol doc](doc/protocol.md) for details.

## Supported Features

* Asyncronous and Syncronous processing
	* Submit a job and grab a result at a later time up to [TTL](#ttl---time-to-live).
	* Submit a job and wait for the result synchronously ([Gearman](http://gearman.org) style).
* Adhoc job scheduling at any future UTC time.
* Numeric job priorities within 2^32, 0 being the default priority (lowest).
* Per job [TTR](#ttr---time-to-run) (time-to-run) - Limits max execution time in milliseconds for a single attempt until re-queue.
* Per job [TTL](#ttl---time-to-live) expiration - Limits max job lifetime in milliseconds until automatic deletion.
* Per job retry support with [max-attempts](#max-attempts) and [max-fails](#max-fails) flags.

### Roadmapped

- [ ] Command Log (Data Persistence).
- [ ] Performance Benchmarks


## What is Workq useful for?

**Scheduling & Timers**

Schedule adhoc one-time jobs at a future UTC time. Workq can be used as a timer for any application event scheduling. Compare this to the usual alternative of building scheduling schema into an existing datastore, writing a custom worker to poll the datastore, and finally building in locking & state management to manage dispatching.

In Workq, you [schedule the job](#schedule-a-job), [lease the job](##leasing-jobs), and [complete the job](#completing-jobs).

**Concurrency Control**

Workq can enable concurrency especially in languages that do not have built-in concurrency. [Background multiple jobs](#add-a-job-background), [process them with multiple workers](#leasing-jobs), and [retrieve results](#retrieving-job-results) from within a single process.

**Streamlining & Persistent Processing**

Workq will retry jobs from [TTR](#ttr---time-to-run) (time-to-run) timeouts and/or from explicit job failures through the [max-attempts](#max-attempts) and [max-fails](#max-fails) flags. Retry is a job execution specification and does not require any custom worker logic. 

**Distributing Work**

Distribute work to multiple workers using Workq as the coordinator. In addition, Workq is language agnostic, submit a job from one language and process it in another.

Workq can also naturally store job execution results for later retrieval up to the job's [TTL](#ttl---time-to-live).

## Project Status

Workq is in alpha status and not yet stable. The protocol may still be subject to small changes. Full unit test coverage, system testing, and fuzz testing are used to ensure maximum coverage and safety throughout development.
Workq is currently suitable for development experimentation and evaluation.  Stabilization will happen once the test coverage is thorough enough and there no major gaps in the protocol. There will be absolutely no reliance on manual testing where technically possible. Workq will slow bake until ready.

**Table of Contents**

- [Getting Started](#getting-started)
	- [Installing](#installing)
	- [Starting](#starting)
	- [Connecting](#connecting)
	- [Submitting Jobs - 3 Ways](#submitting-jobs---3-ways)
		- [Add a Job (Background)](#add-a-job-background)
		- [Run a Job (Foreground)](#run-a-job-foreground)
		- [Schedule a Job](#schedule-a-job)
	- [Leasing Jobs](#leasing-jobs)
	- [Completing Jobs](#completing-jobs)
	- [Failing Jobs](#failing-jobs)
	- [Retrieving Job Results](#retrieving-job-results)
	- [And More](#and-more)
- [Clients](#clients)
- [Caveats & Limitations](#caveats-&-limitations)
- [Glossary](#glossary)
	- [Priority](#priority)
	- [TTR - Time-to-run](#ttr---time-to-run)
	- [TTL - Time-to-live](#ttl---time-to-live)
	- [Lease](#lease)
	- [Max-Attempts](#max-attempts)
	- [Max-Fails](#max-fails)
- [Contributing](#contributing)
	- [Contribution Suitability](#contribution-suitability)
	- [Bug Reports](#bug-reports)
	- [Feature Requests](#feature-requests)
- [Supporting Workq](#supporting-workq)
- [Credits](#credits)
- [Thanks](#thanks)


## Getting Started

Client/Worker examples are shown using the [Go client](https://github.com/iamduo/go-workq).

### Installing

```
make server
# Builds into bin/workq-server
```

### Starting

```
bin/workq-server
```

Starts a workq-server on port 9922.

### Connecting

```go
import "github.com/iamduo/go-workq"
// ...

client, err := workq.Connect("localhost:9922")
if err != nil {
  // ...
}
```

### Submitting Jobs - 3 Ways

#### Add a Job (Background)

[Protocol Doc](doc/protocol.md#add) | [Go Doc](https://godoc.org/github.com/iamduo/go-workq#Client.Add)

Add a background job. The result can be retrieved through the ["result"](#retrieving-job-results) command.

```go
job := &workq.BgJob{
	ID: "6ba7b810-9dad-11d1-80b4-00c04fd430c4",
	Name: "ping",
	TTL: 60000,      // Expire after 60 seconds
	TTR: 5000,       // 5 second time-to-run limit
	Payload: []byte("ping"),
	Priority: 10,    // @OPTIONAL Numeric priority, default 0.
	MaxAttempts: 3,  // @OPTIONAL Absolute max num of attempts.
	MaxFails: 1,     // @OPTIONAL Absolute max number of failures.
}
err := client.Add(job)
if err != nil {
	// ...
}
```

#### Run a Job (Foreground)

[Protocol Doc](doc/protocol.md#run) | [Go Doc](https://godoc.org/github.com/iamduo/go-workq#Client.Run)

Run a job and wait for its result.

```go
job := &workq.FgJob{
	ID: "6ba7b810-9dad-11d1-80b4-00c04fd430c4",
	Name: "ping",
	TTR: 5000,          // 5 second time-to-run limit
	Timeout: 60000, 		// Wait up to 60 seconds for a worker to pick up.
	Payload: []byte("ping"),
	Priority: 10,       // @OPTIONAL Numeric priority, default 0.
}
result, err := client.Run(job)
if err != nil {
  // ...
}

fmt.Printf("Success: %t, Result: %s", result.Success, result.Result)
```

#### Schedule a Job

[Protocol Doc](doc/protocol.md#schedule) | [Go Doc](https://godoc.org/github.com/iamduo/go-workq#Client.Schedule)

Schedule a job at a UTC time. The result can be retrieved through the ["result"](#retrieving-job-results) command.

```go
job := &workq.ScheduledJob{
	ID: "6ba7b810-9dad-11d1-80b4-00c04fd430c4",
	Name: "ping",
	Time: "2016-12-01T00:00:00Z"    // Start job at this UTC time.
	TTL: 60000,                     // Expire after 60 seconds
	TTR: 5000,                      // 5 second time-to-run limit
	Payload: []byte("ping"),
	Priority: 10,                   // @OPTIONAL Numeric priority, default 0.
	MaxAttempts: 3,                 // @OPTIONAL Absolute max num of attempts.
	MaxFails: 1,                    // @OPTIONAL Absolute max number of failures.
}
err := client.Schedule(job)
if err != nil {
	// ...
}
```

### Leasing Jobs

[Protocol Doc](doc/protocol.md#lease) | [Go Doc](https://godoc.org/github.com/iamduo/go-workq#Client.Lease)

Lease the first job within a set of one or more job names with a wait-timeout (milliseconds).

```go
// Lease the first available job in "ping1", "ping2", "ping3"
// waiting up to 60 seconds.
job, err := client.Lease([]string{"ping1", "ping2", "ping3"}, 60000)
if err != nil {
	// ...
}

fmt.Printf("Leased Job: ID: %s, Name: %s, Payload: %s", job.ID, job.Name, job.Payload)
```

### Completing Jobs

[Protocol Doc](doc/protocol.md#complete) | [Go Doc](https://godoc.org/github.com/iamduo/go-workq#Client.Complete)

Mark a job successfully completed with a result.

```go
err := client.Complete("6ba7b810-9dad-11d1-80b4-00c04fd430c4", []byte("pong"))
if err != nil {
	// ...
}
```

### Failing Jobs

[Protocol Doc](doc/protocol.md#fail) | [Go Doc](https://godoc.org/github.com/iamduo/go-workq#Client.Fail)

Mark a job failed with a result.

```go
err := client.Fail("6ba7b810-9dad-11d1-80b4-00c04fd430c4", []byte("failed"))
if err != nil {
	// ...
}
```
### Retrieving Job Results

[Protocol Doc](doc/protocol.md#result) | [Go Doc](https://godoc.org/github.com/iamduo/go-workq#Client.Result)

Get a job result previously executed by [Add](#run-a-job-foreground) or [Schedule](#schedule-a-job) commands.

```go
// Get a job result, waiting up to 60 seconds if the job is still executing.
result, err := client.Result("6ba7b810-9dad-11d1-80b4-00c04fd430c4", 60000)
if err != nil {
	// ...
}

fmt.Printf("Success: %t, Result: %s", result.Success, result.Result)
```

### And More

Please see the full [protocol doc](doc/protocol.md) and [Go client docs](https://github.com/iamduo/go-workq) for more details.

## Clients

* [Go](https://github.com/iamduo/workq)

### Developing New Clients

While there is no official client development guide yet, developing a Workq client is fairly straightforward simply by reading the [protocol doc](doc/protocol.md). In addition, you can review the [Go client](https://github.com/iamduo/workq) as a working example. If you have questions, please join our [mailing list](https://groups.google.com/d/forum/workq).

## Caveats & Limitations

Workq can't and will not do everything you want. Some things to keep in mind:

* In-memory only *for now*, disk backed durability is on the roadmap.
* Job payload & results are limited to 1 MiB each.
* Workq servers are standalone and do not speak to each other.

## Glossary

### Priority

Priorities are numeric from 0 (default, and lowest) - 4,294,967,295 (highest, 2^32) and can control the order of job execution within a single job queue. Higher priorities are executed first.

### TTR - Time-to-run

TTR stands for time-to-run and is the maximum time a job can run for after being [leased](#leasing-jobs) before it is requeued. This value is in milliseconds.

### TTL - Time-to-live

Expiration time before job is deleted regardless of state. This value is in milliseconds.

### Lease

A worker receives work by leasing jobs by their name. A lease is bound by the job's [TTR](#ttr---time-to-run).

### Max-Attempts

Max Attempts is used in `add` and `schedule` commands to specify the number of times a job can be attempted before being marked as failed. An attempt is counted anytime a job is leased regardless of the outcome. This primarily is a job timing out due to [TTR](#ttr---time-to-run).

### Max-Fails

Max number of explicit job failures. Explicit job failures are not from timeouts, but from explicit workers reporting results by the `fail` command. Use this when you want a job to be retried on "true" worker failures and not timeouts from [TTR](#ttr---time-to-run).

## Contributing

* Don't violate [DRY](http://programmer.97things.oreilly.com/wiki/index.php/Don%27t_Repeat_Yourself) principles.
* [Boy Scout Rule](http://programmer.97things.oreilly.com/wiki/index.php/The_Boy_Scout_Rule) needs to have been applied.
* Your code should look like all the other code – this project should look like it was written by one person, always.
* If you want to propose something – just create an issue and describe your question with as much description as you can.
* Avoid sending blind pull requests, have a discussion first. A new feature pull request should not come as a surprise to the maintainer.
* If you add new code, it should be covered by tests fully. No tests – no code.
* If you add a new feature, don't forget to update the documentation for it. No documentation, no feature.
* If you find a bug (or at least you think it is a bug), create an issue with the version and test case that can be run or at least full reproduction steps of the bug.

### Contribution Suitability

The project maintainer has the last word on whether or not a contribution is
suitable for Workq. All contributions will be considered carefully, but from
time to time, contributions will be rejected because they do not suit the
current goals or needs of the project.

If your contribution is rejected, don't despair! As long as you followed these
guidelines, you will have a much better chance of getting your next
contribution accepted.

### Bug Reports

Bug reports are hugely important! Before you raise one, though, please check through the GitHub issues, both open and closed, to confirm that the bug hasn't been reported before. Duplicate bug reports are a huge drain on the time of other contributors, and should be avoided as much as possible.

### Feature Requests

Workq is intended to have a small API to solve a very specific set of use cases. Maintaining a small API allows Workq to be maintainable in the future and understandable.

One of the most important skills to have while maintaining an open source project is learning the ability to say "no" to suggested changes, while keeping an open ear and mind.

Admittedly Workq is still in its early stages. If you believe there is a feature missing, feel free to raise a feature request, but please do be aware that not all requests are suitable.

* This guide was adapted/sourced from python [Requests](https://github.com/kennethreitz/requests/blob/46184236dc177fb68c7863445609149d0ac243ea/docs/dev/contributing.rst) and [Automattic/kue](https://github.com/Automattic/kue#contributing).

## Supporting Workq

If enough users find value in Workq, I'll attempt to set up a strategy to sustain it for the long term. If you find value in Workq for your company, please say hello on our [mailing list](https://groups.google.com/d/forum/workq).

## Credits

Workq is inspired from prior works. The following projects have shaped Workq's feature set:

* Beanstalkd - Simple protocol, TTR, and reserve concepts.
* Gearman - Job server concept.
* Memcached - Simple protocol.
* Redis & Disque - Reference point for server design & packaging and many other things.
* SQLite & Varnish - Strategies to sustain open source.
* Rihanna for working so hard...
* Probably many more....

## Thanks

Special thanks to Theofanis M3 Industries, the initial Workq Supporter, which provided me the hardware to build Workq.
