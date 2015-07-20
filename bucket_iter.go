package main

import (
	"flag"
	"github.com/bioothod/bucket-iter/parse"
	"log"
)

type string_slice []string
func (s *string_slice) String() string {
	return "Unused"
}

func (s *string_slice) Set(value string) error {
	*s = append(*s, value)
	return nil
}

func main() {
	var buckets string_slice
	flag.Var(&buckets, "bucket", "File with keys for one bucket (can be specified multiple times)")
	config_file := flag.String("config", "", "Transport config file")
	flag.Parse()

	if len(buckets) == 0 && len(flag.Args()) == 0 {
		log.Fatalf("You must specify file with keys or provide files in command line (without -bucket arg)")
	}

	if *config_file == "" {
		log.Fatal("You must specify config file")
	}

	p := parse.ParserInit(*config_file)

	for _, bname := range buckets {
		p.ParseOneBucketFile(bname)
	}

	buckets = flag.Args()
	for _, bname := range buckets {
		p.ParseOneBucketFile(bname)
	}

	p.PrintStats()
	return
}

