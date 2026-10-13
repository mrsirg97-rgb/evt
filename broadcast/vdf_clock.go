package broadcast

import (
	"context"
	"crypto/sha256"
	"math/big"
	"sort"

	"github.com/mrsirg97-rgb/evt"
	vdf "github.com/mrsirg97-rgb/evt/broadcast/vdf"
)

type VDFClock interface {
	evt.Clock
	Start(ctx context.Context, engine evt.Engine, onTick func(index uint64))
	Proof() *vdf.VDFProof
	Output() *big.Int
	Entries() []Tick
	Record(index uint64, output *big.Int, proof *vdf.VDFProof) bool
	Trust(index uint64, output *big.Int, proof *vdf.VDFProof) bool
	Verify(output, input *big.Int, proof *vdf.VDFProof) bool
}

type vdfClock struct {
	vdf    *vdf.VDF
	seed   *big.Int
	window int64
	voice  int

	index    uint64
	output   *big.Int
	proof    *vdf.VDFProof
	frontier map[uint64]*big.Int
}

func NewVDFClock(prime *big.Int, iterations int64, seed *big.Int, window int64, voice int) VDFClock {
	return &vdfClock{
		vdf:      vdf.NewVDF(prime, iterations),
		seed:     seed,
		window:   window,
		voice:    voice,
		frontier: map[uint64]*big.Int{0: seed},
	}
}

func (c *vdfClock) Step() uint64 { return c.index }

func (c *vdfClock) Proof() *vdf.VDFProof { return c.proof }

func (c *vdfClock) Output() *big.Int { return c.output }

func (c *vdfClock) Verify(output, input *big.Int, proof *vdf.VDFProof) bool {
	return c.vdf.VerifyVDF(output, input, proof)
}

func (c *vdfClock) Record(index uint64, output *big.Int, proof *vdf.VDFProof) bool {
	if index == 0 || output == nil {
		return false
	}
	input, ok := c.frontier[index-1]
	if !ok {
		return false
	}
	if !c.Verify(output, input, proof) {
		return false
	}
	c.frontier[index] = output
	c.advance(index, output, proof)
	return true
}

func (c *vdfClock) Trust(index uint64, output *big.Int, proof *vdf.VDFProof) bool {
	if c.index != 0 || index == 0 || output == nil {
		return false
	}
	c.frontier[index] = output
	c.advance(index, output, proof)
	return true
}

func (c *vdfClock) advance(index uint64, output *big.Int, proof *vdf.VDFProof) {
	if index <= c.index {
		return
	}
	c.index, c.output, c.proof = index, output, proof
	c.compact()
}

func (c *vdfClock) compact() {
	floor := int64(c.index) - c.window - 2
	if floor <= 0 {
		return
	}
	for index := range c.frontier {
		if int64(index) < floor {
			delete(c.frontier, index)
		}
	}
}

func (c *vdfClock) Entries() []Tick {
	indexes := make([]int, 0, len(c.frontier))
	for index := range c.frontier {
		indexes = append(indexes, int(index))
	}
	sort.Ints(indexes)
	out := []Tick{}
	for _, index := range indexes {
		proof := &vdf.VDFProof{}
		if uint64(index) == c.index {
			proof = c.proof
		}
		out = append(out, Tick{Index: uint64(index), Output: c.frontier[uint64(index)], Proof: proof})
	}
	return out
}

type vdfTick struct {
	index  uint64
	output *big.Int
}

func (c *vdfClock) Start(ctx context.Context, engine evt.Engine, onTick func(index uint64)) {
	go func() {
		current := c.seed
		mine := uint64(0)
		for {
			if ctx.Err() != nil {
				return
			}
			output, proof := c.vdf.ComputeVDF(current)
			mine++
			done := make(chan vdfTick, 1)
			engine.Add(evt.Func(func(context.Context) {
				if mine > c.index {
					c.frontier[mine] = output
					c.advance(mine, output, proof)
					if onTick != nil {
						onTick(mine)
					}
				}
				done <- vdfTick{index: c.index, output: c.output}
			}), c.voice)
			select {
			case <-ctx.Done():
				return
			case top := <-done:
				if top.index >= mine {
					current, mine = top.output, top.index
				} else {
					current = output
				}
			}
		}
	}()
}

func GenesisSeed(roomId string) *big.Int {
	sum := sha256.Sum256([]byte(roomId))
	seed := new(big.Int).SetBytes(sum[:])
	if seed.Cmp(big.NewInt(2)) < 0 {
		seed.SetInt64(2)
	}
	return seed
}

func RFC3526Prime1536() *big.Int {
	p, _ := new(big.Int).SetString(
		"FFFFFFFFFFFFFFFFC90FDAA22168C234C4C6628B80DC1CD129024E088A67CC74"+
			"020BBEA63B139B22514A08798E3404DDEF9519B3CD3A431B302B0A6DF25F1437"+
			"4FE1356D6D51C245E485B576625E7EC6F44C42E9A637ED6B0BFF5CB6F406B7ED"+
			"EE386BFB5A899FA5AE9F24117C4B1FE649286651ECE45B3DC2007CB8A163BF05"+
			"98DA48361C55D39A69163FA8FD24CF5F83655D23DCA3AD961C62F356208552BB"+
			"9ED529077096966D670C354E4ABC9804F1746C08CA237327FFFFFFFFFFFFFFFF", 16)
	return p
}

func RSA2048() *big.Int {
	n, _ := new(big.Int).SetString(
		"2519590847565789349402718324004839857142928212620403202777713783604366202070"+
			"7595556264018525880784406918290641249515082189298559149176184502808489120072"+
			"8449926873928072877767359714183472702618963750149718246911650776133798590957"+
			"0009733045974880842840179742910064245869181719511874612151517265463228221686"+
			"9987549182422433637259085141865462043576798423387184774447920739934236584823"+
			"8242811981638150106748104516603773060562016196762561338441436038339044149526"+
			"3443219011465754445417842402092461651572335077870774981712577246796292638635"+
			"6373289912154831438167899885040445364023527381951378636564391212010397122822"+
			"120720357", 10)
	return n
}
