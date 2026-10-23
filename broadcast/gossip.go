package broadcast

import (
	"encoding/binary"
	"hash/fnv"
	"sort"
)

const meshBuckets = 1 << 12

type Summary struct {
	hash map[int]uint64
	keys map[int][]Mint
}

func Summarize(mints []Mint) *Summary {
	s := &Summary{hash: map[int]uint64{}, keys: map[int][]Mint{}}
	for _, mint := range mints {
		b := mintBucket(mint)
		s.keys[b] = append(s.keys[b], mint)
	}
	for b, ks := range s.keys {
		sort.Slice(ks, func(i, j int) bool { return mintLess(ks[i], ks[j]) })
		h := fnv.New64a()
		var buf [24]byte
		for _, mint := range ks {
			mintBytes(buf[:], mint)
			h.Write(buf[:])
		}
		s.hash[b] = h.Sum64()
	}
	return s
}

func mintBucket(mint Mint) int {
	h := fnv.New64a()
	var buf [24]byte
	mintBytes(buf[:], mint)
	h.Write(buf[:])
	return int(h.Sum64() % meshBuckets)
}

func mintBytes(buf []byte, mint Mint) {
	binary.BigEndian.PutUint64(buf[0:8], uint64(mint.Step))
	binary.BigEndian.PutUint64(buf[8:16], uint64(mint.Member))
	binary.BigEndian.PutUint64(buf[16:24], uint64(mint.Nonce))
}
