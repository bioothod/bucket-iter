package main

import (
	"flag"
	"github.com/bioothod/bucket-iter/parse"
	"log"
)

func main() {
	keys := flag.String("keys", "", "File with keys")
	config_file := flag.String("config", "", "Transport config file")
	flag.Parse()

	if *keys == "" {
		log.Fatalf("You must specify keys file")
	}

	if *config_file == "" {
		log.Fatal("You must specify config file")
	}

	p := parse.ParserInit(*config_file)

	p.ParseFile(*keys)
	return
}

