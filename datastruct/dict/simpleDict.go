package dict

type SimpleDict struct {
	m map[string]interface{}
}

func MakeSimpleDict() *SimpleDict {
	return &SimpleDict{
		m: make(map[string]interface{}),
	}
}

func (d *SimpleDict) Get(key string) (val interface{}, exists bool) {
	val, exists = d.m[key]
	return
}

func (d *SimpleDict) Len() int {
	return len(d.m)
}

func (d *SimpleDict) Put(key string, val interface{}) (result int) {
	// kv 已经存在 插入 return 0
	// kv 不存在 插入 return 1
	_, ok := d.m[key]
	if !ok {
		d.m[key] = val
		return 1
	} else {
		d.m[key] = val
		return 0
	}
}

func (d *SimpleDict) PutIfAbsent(key string, val interface{}) (result int) {
	// kv 已经存在 不插入 return 0
	// kv 不存在 插入 return 1
	_, ok := d.m[key]
	if !ok {
		d.m[key] = val
		result = 1
		return
	} else {
		result = 0
		return
	}
}

func (d *SimpleDict) PutIfExists(key string, val interface{}) (result int) {
	// kv 已经存在 插入 return 1
	// kv 不存在 不插入 return 0
	_, ok := d.m[key]
	if ok {
		d.m[key] = val
		return 1
	} else {
		return 0
	}
}

func (d *SimpleDict) Remove(key string) (result int) {
	_, ok := d.m[key]
	delete(d.m, key)
	if ok {
		return 1
	} else {
		return 0
	}
}

func (d *SimpleDict) ForEach(consumer Consumer) {
	for key, val := range d.m {
		if !consumer(key, val) {
			break
		}
	}
}

func (d *SimpleDict) Keys() []string {
	keys := make([]string, d.Len())
	i := 0
	for key := range d.m {
		keys[i] = key
		i++
	}
	return keys
}

func (d *SimpleDict) RandomKeys(limit int) []string {
	res := make([]string, limit)
	for i := 0; i < limit; i++ {
		for key := range d.m {
			res[i] = key
			break
		}
	}
	return res
}

func (d *SimpleDict) RandomDistinctKeys(limit int) []string {
	keys := d.Keys()
	if len(keys) > limit {
		return keys[:limit]
	} else {
		return keys
	}
}

func (d *SimpleDict) Clear() {
	*d = *MakeSimpleDict()
}
