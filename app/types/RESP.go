package types

type RESPType int

const (
	SimpleString RESPType = iota
	Error
	Integer
	BulkString
	Array
)

type RESPValue struct {
	Type  RESPType
	Value interface{}
}
