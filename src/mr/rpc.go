package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	Wait
	Exit
)

type TaskStatus int

const (
	MapSuccess = iota
	ReduceSuccess
	MapFailure
	ReduceFailure
)

type MessageArgs struct {
	TaskID     int
	TaskStatus TaskStatus
}
type MessageReply struct {
	TaskID   int
	TaskType TaskType
	TaskFile string
	NMap     int
	NReduce  int
}
