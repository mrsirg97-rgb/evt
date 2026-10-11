package broadcast

type Encoder interface {
	Encode(message Message) ([]byte, error)
	Decode(encoded []byte) (Message, error)
}
