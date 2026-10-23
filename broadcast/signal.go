package broadcast

import (
	"encoding/json"

	"github.com/mrsirg97-rgb/evt"
)

type Claim struct {
	evt.Mark
	Talk int64
}

type Release struct {
	evt.Mark
	Claim Mint
}

type Admission struct {
	evt.Mark
	Act Mint
}

type Acknowledgement struct {
	evt.Mark
	Mint    Mint
	Applied bool
}

type Pinned struct {
	evt.Mark
	Admission Mint
	Act       Mint
}

type Withdrawn struct {
	evt.Mark
	Admission Mint
}

type Left struct {
	evt.Mark
	Member int64
	Answer bool
}

type Hold struct {
	Claim    Mint
	Priority int
	Talk     int64
	Beyond   bool
}

type Checkpoint struct {
	evt.Mark
	Key   int64
	Head  int64
	Holds []Hold
}

type Joined struct {
	evt.Mark
}

type Seated struct {
	evt.Mark
	Live []int64
}

func Protocol() (Registry, error) {
	reg, err := jsonCodec[Claim](NewRegistry(), "claim")
	if err != nil {
		return nil, err
	}
	reg, err = jsonCodec[Release](reg, "release")
	if err != nil {
		return nil, err
	}
	reg, err = jsonCodec[Admission](reg, "admission")
	if err != nil {
		return nil, err
	}
	reg, err = jsonCodec[Acknowledgement](reg, "acknowledgement")
	if err != nil {
		return nil, err
	}
	reg, err = jsonCodec[Pinned](reg, "pinned")
	if err != nil {
		return nil, err
	}
	reg, err = jsonCodec[Withdrawn](reg, "withdrawn")
	if err != nil {
		return nil, err
	}
	reg, err = jsonCodec[Left](reg, "left")
	if err != nil {
		return nil, err
	}
	reg, err = jsonCodec[Checkpoint](reg, "checkpoint")
	if err != nil {
		return nil, err
	}
	reg, err = jsonCodec[Joined](reg, "joined")
	if err != nil {
		return nil, err
	}
	return jsonCodec[Seated](reg, "seated")
}

func jsonCodec[T evt.Signal](r Registry, kind string) (Registry, error) {
	return Declare[T](r, kind,
		func(v T) ([]byte, error) { return json.Marshal(v) },
		func(raw []byte) (T, error) {
			var v T
			if err := json.Unmarshal(raw, &v); err != nil {
				return v, err
			}
			return v, nil
		})
}
