package set

import (
	"memgo/datastruct/dict"
)

type Set struct {
	dict dict.DictIntf
}

func MakeSet(members ...string) *Set {
	set := &Set{
		dict: dict.MakeSimpleDict(),
	}
	for _, member := range members {
		set.Add(member)
	}
	return set
}

func (s *Set) Add(val string) int {
	return s.dict.Put(val, nil)
}

func (s *Set) Remove(val string) int {
	return s.dict.Remove(val)
}

func (s *Set) IsMember(val string) bool {
	_, exists := s.dict.Get(val)
	return exists
}

func (s *Set) Len() int {
	return s.dict.Len()
}

func (s *Set) ForEach(consumer func(member string) bool) {
	s.dict.ForEach(func(key string, val interface{}) bool {
		return consumer(key)
	})
}

// dict的ForEach方法是有可能遍历到新添加的key的
func (s *Set) ToSlice() []string {
	sli := make([]string, s.Len())
	i := 0
	s.dict.ForEach(func(key string, val interface{}) bool {
		if i < len(sli) {
			sli[i] = key
		} else {
			sli = append(sli, key)
		}
		i++
		return true
	})
	return sli
}

func (s *Set) Intersect(another *Set) *Set {
	// TODO implement it
	res := MakeSet()
	another.ForEach(func(member string) bool {
		if s.IsMember(member) {
			res.Add(member)
		}
		return true
	})
	return res
}

func (s *Set) Union(another *Set) *Set {
	res := MakeSet(s.ToSlice()...)
	another.ForEach(func(member string) bool {
		res.Add(member)
		return true
	})
	return res
}

func (s *Set) Diff(another *Set) *Set {
	res := MakeSet()
	s.ForEach(func(member string) bool {
		if !another.IsMember(member) {
			res.Add(member)
		}
		return true
	})
	return res
}

func (s *Set) RandomMembers(limit int) []string {
	return s.dict.RandomKeys(limit)
}

func (s *Set) RandomDistinctMembers(limit int) []string {
	return s.dict.RandomDistinctKeys(limit)
}
