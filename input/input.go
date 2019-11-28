package input

type Input interface {
	Init() error
	Msgs() chan []byte
	Start() error
	Stop() error
}
