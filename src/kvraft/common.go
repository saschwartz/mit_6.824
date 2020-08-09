package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key          string
	Value        string
	Op           string // "Put" or "Append"
	ClientSerial int
	ClientID     string
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key          string
	ClientSerial int
	ClientID     string
}

type GetReply struct {
	Err   Err
	Value string
}

// LogLevel state types and consts
type LogLevel int

// LogDebug etc are different levels we can log at
const (
	LogDebug LogLevel = iota
	LogInfo
	LogWarning
	LogError
)

func (me LogLevel) String() string {
	return [...]string{"DEBUG", "INFO", "WARNING", "ERROR"}[me]
}

// SetLogLevel sets the level we log at
const (
	SetLogLevel LogLevel = LogDebug
)
