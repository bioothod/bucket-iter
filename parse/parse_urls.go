package parse

import (
	"bufio"
	"github.com/bioothod/backrunner/config"
	"github.com/bioothod/backrunner/bucket"
	"github.com/bioothod/backrunner/etransport"
	//"github.com/bioothod/backrunner/reply"
	"github.com/bioothod/elliptics-go/elliptics"
	"fmt"
	"log"
	"os"
	"strings"
)

type BucketStat struct {
	// hashed object name to its size mapping
	MatchedSize map[string]uint64
	Keys uint64

	TotalMatchedSize uint64
	TotalUnmatchedSize uint64
}

func NewBucketStat() *BucketStat {
	return &BucketStat {
		MatchedSize: make(map[string]uint64),
		TotalMatchedSize: 0,
		TotalUnmatchedSize: 0,
	}
}

func (stat *BucketStat) InsertSize(name string, size uint64) {
	stat.MatchedSize[name] = size
	return
}

func (stat *BucketStat) Insert(name string) {
	stat.InsertSize(name, 0)
	return
}

func (stat *BucketStat) MatchSize(name string, size uint64) {
	_, ok := stat.MatchedSize[name]
	if ok {
		stat.InsertSize(name, size)
		stat.TotalMatchedSize += size
	} else {
		stat.TotalUnmatchedSize += size
	}
}

type ParserCtl struct {
	Ell *etransport.Elliptics
	Session *elliptics.Session
	Stat *elliptics.DnetStat

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

	p.Ell, err = etransport.NewEllipticsTransport(conf)
	if err != nil {
		log.Fatalf("Could not create Elliptics transport: %v", err)
	}

	p.Session, err = elliptics.NewSession(p.Ell.Node)
	if err != nil {
		log.Fatalf("Could not create Elliptics session: %v", err)
	}

	p.Stat, err = p.Ell.Stat()
	if err != nil {
		log.Fatal("Could not read statistics: %v", err)
	}

	return p
}

func (p *ParserCtl) BucketCheck(bname string) (b *bucket.Bucket, err error) {
	b, err = bucket.ReadBucket(p.Ell, bname)
	if err != nil {
		log.Printf("bucket_check: could not read bucket '%s': %v", bname, err)
		return nil, err
	}

	for _, group_id := range b.Meta.Groups {
		sg, ok := p.Stat.Group[group_id]
		if ok {
			b.Group[group_id] = sg
		} else {
			log.Printf("bucket_check: bucket: %s: there is no group %d in stats", bname, group_id)
			return nil, fmt.Errorf("bucket: %s: there is no group %d in stats", bname, group_id)
		}
	}

	log.Printf("bucket_check: %s, groups: %v\n", bname, b.Meta.Groups);
	return b, err
}

func (p *ParserCtl) ParseOneBucketFile(file string) (err error) {
	in, err := os.Open(file)
	if err != nil {
		log.Printf("Could not open file %s: %v\n", file, err)
		return err
	}
	defer in.Close()

	var keys uint64

	scanner := bufio.NewScanner(in)
	for scanner.Scan() {
		line := scanner.Text()
		sb := strings.SplitN(line, "/", 2)
		if len(sb) == 2 {
			stat, ok := p.Buckets[sb[0]]
			if !ok {
				stat = NewBucketStat()
				p.Buckets[sb[0]] = stat
			}
			//stat.Insert(sb[1])
			stat.Keys += 1
			keys += 1
		}
	}

	if err = scanner.Err(); err != nil {
		log.Printf("Could not read file: %s: %v\n", file, err)
		return err
	}

	log.Printf("bucket-file: %s, inserted-keys: %d\n", file, keys)

	return err
}

func (p *ParserCtl) ParseFile(file string) (err error) {
	in, err := os.Open(file)
	if err != nil {
		log.Printf("Could not open file %s: %v\n", file, err)
		return err
	}
	defer in.Close()

	scanner := bufio.NewScanner(in)
	for scanner.Scan() {
		line := scanner.Text()
		s := strings.Split(line, "/get/")

		if len(s) == 2 {
			if strings.HasPrefix(s[1], "p") {
				log.Printf("invalid name: %s\n", s[1])
				continue
			}

			sb := strings.SplitN(s[1], "/", 2)
			if len(sb) == 2 {
				stat, ok := p.Buckets[sb[0]]
				if !ok {
					stat = NewBucketStat()
					p.Buckets[sb[0]] = stat
				}
				stat.Insert(sb[1])
			}
		}
	}

	if err = scanner.Err(); err != nil {
		log.Printf("Could not read file: %s: %v\n", file, err)
		return err
	}
	return err
}

func (p *ParserCtl) PrintStats() {
	var total_used_size, total_real_used_size, total_disk_size uint64
	var total_real_records, total_file_records int64

	tb := func(sz uint64) float64 {
		return float64(sz) / float64(1024 * 1024 * 1024 * 1024)
	}

	for bname, stat := range p.Buckets {
		b, err := p.BucketCheck(bname)
		if err != nil {
			log.Printf("bucket: %s: could not read bucket data and stats: %v\n", bname, err)
			continue
		}

		total_file_records += int64(stat.Keys)

		for group_id, sg := range b.Group {
			var used_size, removed_size uint64
			var records, removed_records uint64

			used_size = 0
			removed_size = 0

			records = 0
			removed_records = 0

			for _, sb := range sg.Ab {
				used_size += sb.VFS.BackendUsedSize
				removed_size += sb.VFS.BackendRemovedSize

				records += sb.VFS.RecordsTotal
				removed_records += sb.VFS.RecordsRemoved

				total_disk_size += sb.VFS.TotalSizeLimit
			}

			real_size := used_size - removed_size

			records_real := int64(records - removed_records)
			diff := records_real - int64(stat.Keys)
			percentage := float64(diff) / float64(records_real) * 100

			total_used_size += used_size
			total_real_used_size += real_size
			total_real_records += records_real

			fmt.Printf("bucket: %s, files: %d, stat: group: %d, used-size: %d (%.2f Tb), removed-size: %d (%.2f Tb), real-used-size: %d (%.2f Tb), records: %d, removed-records: %d, real-records: %d, diff-with-file: %d, percentage: %.2f%%\n",
				bname, stat.Keys,
				group_id,
				used_size, tb(used_size), removed_size, tb(removed_size), real_size, tb(real_size),
				records, removed_records, records_real,
				diff, percentage)

			break
		}
	}

	diff := total_real_records - total_file_records
	percentage := float64(diff) / float64(total_real_records) * 100

	fmt.Printf("buckets: %d, stat: used-size: %.2f Tb, real-used-size: %.2f Tb, total-disk-size: %.2f Tb, real-records: %d, provided-file-records: %d, diff: %d, percentage: %.2f%%\n",
		len(p.Buckets),
		tb(total_used_size), tb(total_real_used_size), tb(total_disk_size),
		total_real_records, total_file_records,
		diff, percentage)

	return
}
