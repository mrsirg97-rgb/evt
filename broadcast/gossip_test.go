package broadcast

import "testing"

func TestSummaryBucketsSpreadTheMints(t *testing.T) {
	mints := make([]Mint, 0, 64)
	for nonce := range int64(64) {
		mints = append(mints, Mint{Step: 1, Member: 1, Nonce: nonce})
	}
	if buckets := len(Summarize(mints).hash); buckets < 32 {
		t.Fatalf("64 distinct mints must spread over the buckets, landed in %d", buckets)
	}
}

func TestSummariesDifferOnlyInTheBucketThatDiffers(t *testing.T) {
	mints := []Mint{{Step: 1, Member: 1}, {Step: 2, Member: 2}, {Step: 3, Member: 3}}
	extra := Mint{Step: 4, Member: 4}
	mine, theirs := Summarize(mints), Summarize(append(append([]Mint{}, mints...), extra))
	differ := 0
	for b, h := range theirs.hash {
		if mine.hash[b] != h {
			differ++
			if b != mintBucket(extra) {
				t.Fatalf("bucket %d differs, only the extra mint's bucket %d may", b, mintBucket(extra))
			}
		}
	}
	if differ != 1 {
		t.Fatalf("one added mint differs in exactly one bucket, got %d", differ)
	}
}
