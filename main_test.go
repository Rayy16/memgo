package main

import (
	"memgo/database"
	"memgo/redis/RESP/connection"
	utils2 "memgo/utils"
	utils "memgo/utils/rand_string"
	"sync"
	"testing"
)

var testDbserber = database.NewMemgoServer()

func TestSet(t *testing.T) {
	wg := sync.WaitGroup{}
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			fakeConn := &connection.Connection{}
			for k := 0; k < 100; k++ {
				key := utils.RandString(100000)
				value := utils.RandString(100000)
				testDbserber.Exec(fakeConn, utils2.ToCmdLine2("set", key, value))
			}
			wg.Done()
		}()
	}
	wg.Wait()
}
