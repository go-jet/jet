package qrm

import "reflect"

type typeStack []*reflect.Type

func newTypeStack() typeStack {
	stack := make(typeStack, 0, 20)
	return stack
}

func (s *typeStack) isEmpty() bool {
	return len(*s) == 0
}

func (s *typeStack) push(t *reflect.Type) {
	*s = append(*s, t)
}

func (s *typeStack) pop() bool {
	if s.isEmpty() {
		return false
	}
	*s = (*s)[:len(*s)-1]
	return true
}

func (s *typeStack) contains(t *reflect.Type) bool {
	if s.isEmpty() {
		return false
	}

	for _, typ := range *s {
		if *typ == *t {
			return true
		}
	}

	return false
}
