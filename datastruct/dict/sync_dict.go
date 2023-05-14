package dict

import (
	"sync"
	"sync/atomic"
)

type SyncDict struct {
	m     sync.Map
	count int32
}

func MakeSyncDict() *SyncDict {
	return &SyncDict{
		m:     sync.Map{},
		count: 0,
	}
}

func (d *SyncDict) addCount() {
	atomic.AddInt32(&d.count, 1)
}

func (d *SyncDict) decreaseCount() {
	atomic.AddInt32(&d.count, -1)
}

func (d *SyncDict) Get(key string) (val interface{}, exists bool) {
	val, exists = d.m.Load(key)
	return
}

func (d *SyncDict) Len() int {
	return int(atomic.LoadInt32(&d.count))
}

func (d *SyncDict) Put(key string, val interface{}) (result int) {
	// kv 已经存在 插入 return 0
	// kv 不存在 插入 return 1
	_, ok := d.m.Load(key)
	if !ok {
		d.m.Store(key, val)
		d.addCount()
		return 1
	} else {
		d.m.Store(key, val)
		return 0
	}
}

func (d *SyncDict) PutIfAbsent(key string, val interface{}) (result int) {
	// kv 已经存在 不插入 return 0
	// kv 不存在 插入 return 1
	_, ok := d.m.Load(key)
	if !ok {
		d.m.Store(key, val)
		d.addCount()
		result = 1
		return
	} else {
		result = 0
		return
	}
}

func (d *SyncDict) PutIfExists(key string, val interface{}) (result int) {
	// kv 已经存在 插入 return 1
	// kv 不存在 不插入 return 0
	_, ok := d.m.Load(key)
	if ok {
		d.m.Store(key, val)
		return 1
	} else {
		return 0
	}
}

func (d *SyncDict) Remove(key string) (result int) {
	_, ok := d.m.Load(key)
	d.m.Delete(key)
	if ok {
		d.decreaseCount()
		return 1
	} else {
		return 0
	}
}

func (d *SyncDict) ForEach(consumer Consumer) {
	d.m.Range(func(key, value interface{}) bool {
		return consumer(key.(string), value)
	})
}

func (d *SyncDict) Keys() []string {
	keys := make([]string, d.Len())
	i := 0
	d.m.Range(func(key, value interface{}) bool {
		keys[i] = key.(string)
		i++
		return true
	})
	return keys
}

func (d *SyncDict) RandomKeys(limit int) []string {
	keys := make([]string, limit)
	for i := 0; i < limit; i++ {
		d.m.Range(func(key, value interface{}) bool {
			keys[i] = key.(string)
			return false
		})
	}
	return keys
}

func (d *SyncDict) RandomDistinctKeys(limit int) []string {
	if limit >= d.Len() {
		return d.Keys()
	}
	keys := make([]string, limit)
	i := 0
	d.m.Range(func(key, value interface{}) bool {
		keys[i] = key.(string)
		i++
		if i == limit {
			return false
		}
		return true
	})
	return keys
}

func (d *SyncDict) Clear() {
	*d = *MakeSyncDict()
}
