package evt

type Signal interface{ signal() }

type Mark struct{}

func (Mark) signal() {}

type Snapshot interface {
	Signal
	Snapshot()
}

type Level int

const (
	LevelInfo Level = iota
	LevelSuccess
	LevelError
)

func (l Level) String() string {
	switch l {
	case LevelSuccess:
		return "success"
	case LevelError:
		return "error"
	}
	return "info"
}

type Notice struct {
	Source string
	Text   string
	Level  Level
}

func (Notice) signal() {}

var _ Signal = Notice{}
