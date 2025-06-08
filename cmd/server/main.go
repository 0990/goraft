package main

import (
	"flag"
	"fmt"
	"github.com/0990/goraft/server"
	"time"
)

type stringSlice []string

func (s *stringSlice) String() string {
	return fmt.Sprintf("%v", *s)
}

func (s *stringSlice) Set(value string) error {
	*s = append(*s, value)
	return nil
}

func main() {
	var serverCfgs stringSlice
	flag.Var(&serverCfgs, "server", "server cfg paths")
	flag.Parse()

	if len(serverCfgs) == 0 {
		fmt.Println("no servers specified")
		return
	}

	for _, cfg := range serverCfgs {
		go func(cfg string) {
			err := server.Run(cfg)
			if err != nil {
				panic(err)
			}
		}(cfg)
	}

	time.Sleep(time.Hour)
}
