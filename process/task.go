package process

// Task send from feeder to mapper.
type Task struct {
	ID     string
	Msg    interface{}
	Source string
}
