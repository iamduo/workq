package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/iamduo/workq/int/cmd/handlers"
	"github.com/iamduo/workq/int/job"
	"github.com/iamduo/workq/int/log"
	"github.com/iamduo/workq/int/prot"
	"github.com/iamduo/workq/int/server"
	"github.com/iamduo/workq/int/wqueue"
)

var logo = ". . .,---.\n" +
	"| | ||   |\n" +
	"`-'-'`---|\n" +
	"         |\n"

func main() {
	fmt.Printf("%s", logo)
	listen := flag.String("listen", "127.0.0.1:9922", "Listen on HOST:PORT, default 127.0.0.1:9922")
	logFile := flag.String("log-file", "", "Path to log file")
	flag.Parse()

	if *logFile != "" {
		err := initLog(*logFile)
		if err != nil {
			log.ELogger.Println(err)
			os.Exit(1)
		}
	}

	log.OLogger.Printf("Listening on %s", *listen)
	s := buildServer(*listen)
	err := s.ListenAndServe()
	if err != nil {
		log.ELogger.Println(err)
		os.Exit(1)
	}
}

func buildServer(listen string) *server.Server {
	reg := job.NewRegistry()
	controller := wqueue.NewController()
	handlerUsage := &handlers.Usage{}
	serverUsage := &server.Usage{}
	//tracker := tracker.New.(reg, controller)
	hldrs := map[string]server.Handler{
		prot.CmdAdd:      handlers.NewAddHandler(reg, controller, handlerUsage),
		prot.CmdRun:      handlers.NewRunHandler(reg, controller, handlerUsage),
		prot.CmdSchedule: handlers.NewScheduleHandler(reg, controller, handlerUsage),
		prot.CmdDelete:   handlers.NewDeleteHandler(reg, controller),
		prot.CmdLease:    handlers.NewLeaseHandler(reg, controller),
		prot.CmdComplete: handlers.NewCompleteHandler(reg, controller),
		prot.CmdFail:     handlers.NewFailHandler(reg, controller),
		prot.CmdResult:   handlers.NewResultHandler(reg, controller),
		prot.CmdInspect: handlers.NewInspectHandler(
			handlers.NewInspectServerHandler(serverUsage, handlerUsage),
			handlers.NewInspectQueuesHandler(controller),
			handlers.NewInspectQueueHandler(controller),
			handlers.NewInspectJobsHandler(reg, controller),
			handlers.NewInspectJobHandler(reg),
		),
	}
	router := &server.CmdRouter{Handlers: hldrs, UnknownHandler: &handlers.UnknownHandler{}}
	return server.New(listen, router, prot.Prot{}, serverUsage)
}

func initLog(file string) error {
	f, err := os.OpenFile(file, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		return err
	}

	log.OLogger.Printf("Logging errors at %s", file)
	log.ELogger.SetOutput(f)
	return nil
}
