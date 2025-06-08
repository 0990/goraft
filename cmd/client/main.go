package main

import (
	"bufio"
	"flag"
	"github.com/0990/goraft/kvdb"
	"log"
	"os"
	"strings"
)

type Command struct {
	Name   string
	Num    int
	Action func(args []string)
}

const (
	EXIT   = "exit"
	TIP    = "> "
	GET    = "get"
	APPEND = "append"
	PUT    = "put"
	ILLAGM = "illegal argument"
	HUMEN  = `Commands:
	"get k"
	"append k v"
	"put k v"
	"exit"
	`
)

var cfg = flag.String("cfg", "client.yml", "cfg path")

func main() {
	flag.Parse()
	clientEnds := kvdb.GetClientEnds(*cfg)

	client := kvdb.MakeKVClient(clientEnds)

	commands := []*Command{
		{
			Name: GET,
			Num:  2,
			Action: func(args []string) {
				print(client.Get(args[1]))
			},
		},
		{
			Name: APPEND,
			Num:  3,
			Action: func(args []string) {
				client.Append(args[1], args[2])
			},
		},
		{
			Name: PUT,
			Num:  3,
			Action: func(args []string) {
				client.Put(args[1], args[2])
			},
		},
	}

	print(HUMEN)

	for line := readLine(); !strings.EqualFold(line, EXIT); {
		args := parseArgs(line)
		if len(args) > 0 {
			name := args[0]
			do := false
			for _, command := range commands {
				if command.Name != name {
					continue
				}
				do = true
				if len(args) != command.Num {
					print(ILLAGM + HUMEN)
					continue
				}
				command.Action(args)
			}
			if !do {
				print(HUMEN)
			}
		}
		line = readLine()
	}
}

func parseArgs(line string) []string {
	argsT := strings.Split(line, " ")
	args := make([]string, 0)
	for _, str := range argsT {
		if !strings.EqualFold(str, " ") && !strings.EqualFold(str, "") {
			args = append(args, str)
		}
	}
	return args
}

func readLine() string {
	reader := bufio.NewReader(os.Stdin)
	print(TIP)
	answer, _, err := reader.ReadLine()
	if err != nil {
		log.Fatal(err)
	}
	return strings.TrimSpace(string(answer))
}
