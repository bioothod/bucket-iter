package parse

import (
	"bufio"
	"github.com/bioothod/backrunner/etransport"
	"github.com/bioothod/backrunner/reply"
	"github.com/bioothod/elliptics-go/elliptics"
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
)

type BucketStat struct {
	// hashed object name to its size mapping
	MatchedSize map[string]int64
}

func NewBucketStat() *BucketStat {
	return &BucketStat {
		MatchedSize: make(map[string]int64),
	}
}

type ParserCtl struct {
	Ell *etransport.Elliptics
	Session *elliptics.Session

	// bucket name to set of keys in that bucket mapping
	Buckets map[string]*BucketStat
}

func ParserInit(config_file string) (p *ParserCtl) {
	conf := &config.ProxyConfig {}
	err := conf.Load(config_file)
	if err != nil {
		log.Fatalf("Could not load config %s: %q", config_file, err)
	}

	p = &ParserCtl {
		Buckets: make(map[string]*BucketStat),
	}
	return p

	p.Ell, err = etransport.NewEllipticsTransport(conf)
	if err != nil {
		log.Fatalf("Could not create Elliptics transport: %v", err)
	}

	p.Session, err = elliptics.NewSession(e.Ell.Node)
	if err != nil {
		log.Fatalf("Could not create Elliptics session: %v", err)
	}

	return p
}

func (p *ParserCtl) BucketCheck(bname string) (err error) {
	b, err := bucket.ReadBucket(p.Ell, bname)
	if err != nil {
		log.Printf("bucket_check: could not read bucket '%s': %v", bname, err)
		return err
	}

	log.Printf("bucket_check: %s, groups: %v\n", bname, b.Meta.Groups);
	return err
}

func (p *ParserCtl) ParseFile(file string) (err error) {
	in, err := os.Open(file)
	if err != nil {
		log.Printf("Could not open file %s: %v\n", file, err)
		return err
	}
	defer in.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		s := strings.Split(line, "/get/")

		if len(s) == 2 {
			log.Printf("%s\n", s[1])
		}
	}

	if err = scanner.Err(); err != nil {
		log.Printf("Could not read file: %s: %v\n", file, err)
		return err
	}
}
