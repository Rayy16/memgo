package database

import (
	"memgo/interface/database"
	"memgo/interface/resp"
	"strings"
)

type CmdLine = [][]byte

type ExecFunc func(db database.DbObjectIntf, args CmdLine) resp.ReplyIntf

type PreFunc func(args CmdLine) (writeKeys []string, readKeys []string)

var cmdTable = make(map[string]*command)

type command struct {
	executor ExecFunc
	prepare  PreFunc
	arity    int
	// flags    int
}

const (
	flagSpec   = 0
	flagRead   = 1
	flagWrite  = 2
	flagSingle = 4
	flagMulti  = 8
)

func RegisterCommand(name string, executor ExecFunc, prepare PreFunc, arity int) {
	name = strings.ToLower(name)
	cmdTable[name] = &command{
		executor: executor,
		prepare:  prepare,
		arity:    arity,
	}
}

// SET K V =》 arity = 3
// EXISTS k1 k2 k3... arity = -2 (-2 表示这个数能超过 2 )
func validateArity(arity int, cmdArgs CmdLine) bool {
	argNum := len(cmdArgs)
	if arity >= 0 {
		return arity == argNum
	} else { // arity < 0 变长命令
		return argNum >= -arity
	}
}

func isSingleCommand(flags int) bool {
	return flags&8 == 0 && flags&4 > 0
}
func isReadOnlyCommand(flags int) bool {
	return flags&2 == 0 && flags&1 > 0
}
func isSpecCommand(flags int) bool {
	return flags == 0
}

func readFirstKey(args CmdLine) ([]string, []string) {
	// assert len(args) > 0
	key := string(args[0])
	return nil, []string{key}
}

func writeFirstKey(args CmdLine) ([]string, []string) {
	key := string(args[0])
	return []string{key}, nil
}

func writeAllKeys(args CmdLine) ([]string, []string) {
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}
	return keys, nil
}

func readAllKeys(args CmdLine) ([]string, []string) {
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}
	return nil, keys
}

func noPrepare(args CmdLine) ([]string, []string) {
	return nil, nil
}
