// Copyright 2023 Dmitry Dikun
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Types and methods in this package are not thread-safe.

package bptree

import (
	"math"
)

type Key interface {
	int | uint | int8 | int16 | int32 | int64 | uint8 | uint16 | uint32 | uint64 | string
}

type KeyValue[K Key] struct {
	Key   K
	Value any
}

type collision []any

type Iterator[K Key] interface {
	Next() (KeyValue[K], bool)
}

const MinOrder = 3

type BPTree[K Key] struct {
	root *node[K]
	size int
}

// NewBPTree returns a new BPTree. Order measures the capacity of nodes, i.e. maximum allowed
// number of direct child nodes for internal nodes, and maximum key-value pairs for leaf nodes.
// Order should be greater or equal MinOrder, otherwise BPTree will be initialized with MinOrder.
func NewBPTree[K Key](order int) *BPTree[K] {
	if order < MinOrder {
		order = MinOrder
	}
	return &BPTree[K]{
		root: newLeafNode[K](order),
	}
}

// Size returns a number of key-value pairs currently stored in a tree.
func (t *BPTree[K]) Size() int {
	return t.size
}

// Find returns a (value, true) for a given key, or (nil, false) if not found.
func (t *BPTree[K]) Find(key K) (any, bool) {
	if v, ok := t.find(key); ok {
		if v, ok := v.(collision); ok {
			return v[0], true
		}
		return v, true
	}
	return nil, false
}

// FindAll returns a ([]value, true) for a given key, or (nil, false) if not found.
func (t *BPTree[K]) FindAll(key K) ([]any, bool) {
	if v, ok := t.find(key); ok {
		if v, ok := v.(collision); ok {
			return v, true
		}
		return []any{v}, true
	}
	return nil, false
}

func (t *BPTree[K]) find(key K) (any, bool) {
	n := t.root
NodesLoop:
	for n.isInternal() {
		for i, c := range n.children {
			if i == len(n.keys) || key < n.keys[i] {
				n = c
				continue NodesLoop
			}
		}
	}
	for i, k := range n.keys {
		if k == key {
			return n.values[i], true
		}
	}
	return nil, false
}

// Insert puts a key-value pair to the tree. If given key is present in tree, it's value will be replaced.
func (t *BPTree[K]) Insert(key K, val any) {
	t.insert(key, val, true)
}

// Append puts a key-value pair to the tree. If given key is present in tree, val will be appended to it's values.
func (t *BPTree[K]) Append(key K, val any) {
	t.insert(key, val, false)
}

func (t *BPTree[K]) insert(key K, val any, replace bool) {
	n := t.root
	ok, key2, n2 := n.insert(key, val, replace)
	if n2 != nil {
		if n.isLeaf() {
			t.root = newInternalNode[K](cap(n.keys))
		} else {
			t.root = newInternalNode[K](cap(n.children))
		}
		t.root.keys = t.root.keys[:1]
		t.root.keys[0] = key2
		t.root.children = t.root.children[:2]
		t.root.children[0] = n
		t.root.children[1] = n2
	}
	if ok {
		t.size++
	}
}

// Delete removes a key-value pair and returns it's (value, true) if success, or (nil, false) if not found.
// If multiply values are found, last added will be removed.
func (t *BPTree[K]) Delete(key K) (val any, ok bool) {
	return t.delete(key, false, -1)
}

// DeleteOne is like Delete, but removes concrete value if multiply are.
func (t *BPTree[K]) DeleteOne(key K, idx int) (val any, ok bool) {
	return t.delete(key, false, idx)
}


// DeleteAll is like Delete, but removes all values id multiply are.
func (t *BPTree[K]) DeleteAll(key K) (vals []any, ok bool) {
	if v, ok := t.delete(key, true, 0); ok {
		return v.(collision), true
	}
	return nil, false
}

func (t *BPTree[K]) delete(key K, all bool, idx int) (val any, ok bool) {
	val, ok = t.root.delete(key, all, idx)
	if ok {
		if t.root.isInternal() && len(t.root.children) == 1 {
			t.root = t.root.children[0]
		}
		if all {
			c, _ := val.(collision)
			t.size -= len(c)
			return c, true
		} else {
			t.size--
		}
	}
	return
}

type iterator[K Key] struct {
	from *K
	to   *K
	n    *node[K]
	i    int
	c    collision
	ckey K
	ci   int
}

func (i *iterator[K]) Next() (KeyValue[K], bool) {
SEARCH:
	for i.n != nil {
		if i.c != nil {
			if i.ci < len(i.c) {
				kv := KeyValue[K]{Key: i.ckey, Value: i.c[i.ci]}
				i.ci++
				return kv, true
			}
			i.c = nil
		}
		for ; i.i < len(i.n.keys); i.i++ {
			k := i.n.keys[i.i]
			if i.from != nil && k < *i.from {
				continue
			}
			if i.to != nil && k >= *i.to {
				i.n = nil
				break SEARCH
			}
			if c, ok := i.n.values[i.i].(collision); ok {
				i.c = c
				i.ckey = i.n.keys[i.i]
				kv := KeyValue[K]{Key: i.ckey, Value: c[0]}
				i.ci = 1
				i.i++
				return kv, true
			}
			kv := KeyValue[K]{Key: i.n.keys[i.i], Value: i.n.values[i.i]}
			i.i++
			return kv, true
		}
		i.n = i.n.right
		i.i = 0
	}
	return KeyValue[K]{}, false
}

// Iterator returns an Iterator for key-value pairs from interval [*from; *to). Nil given as a parameter will
// be interpreted as begin or end whole tree key diapason.
func (t *BPTree[K]) Iterator(from *K, to *K) Iterator[K] {
	if from != nil && to != nil && *from >= *to {
		return &iterator[K]{}
	}
	n := t.root
NodesLoop:
	for n.isInternal() {
		for i, c := range n.children {
			if from == nil || i == len(n.keys) || *from < n.keys[i] {
				n = c
				continue NodesLoop
			}
		}
	}
	return &iterator[K]{
		from: from,
		to:   to,
		n:    n,
	}
}

// Range returns a slice of key-value pairs from interval [*from; *to). Nil given as a parameter will
// be interpreted as begin or end whole tree key diapason. If there are no keys found, returns nil.
func (t *BPTree[K]) Range(from *K, to *K) []KeyValue[K] {
	i := t.Iterator(from, to)
	var result []KeyValue[K]
	for kv, ok := i.Next(); ok; kv, ok = i.Next() {
		result = append(result, kv)
	}
	return result
}

// Entries returns a slice of all key-value pairs stored in tree. If tree is empty, returns nil.
func (t *BPTree[K]) Entries() []KeyValue[K] {
	return t.Range(nil, nil)
}

// First returns (key-value, true) for the minimal key in tree, or (zero, false) if tree is empty.
func (t *BPTree[K]) First() (KeyValue[K], bool) {
	if t.size == 0 {
		return KeyValue[K]{}, false
	}
	n := t.root
	for n.isInternal() {
		n = n.children[0]
	}
	v := n.values[0]
	if c, ok := v.(collision); ok {
		v = c[0]
	}
	return KeyValue[K]{Key: n.keys[0], Value: v}, true
}

// Last returns (key-value, true) for the maximal key in tree, or (zero, false) if tree is empty.
func (t *BPTree[K]) Last() (KeyValue[K], bool) {
	if t.size == 0 {
		return KeyValue[K]{}, false
	}
	n := t.root
	for n.isInternal() {
		n = n.children[len(n.children)-1]
	}
	v := n.values[len(n.values)-1]
	if c, ok := v.(collision); ok {
		v = c[len(c)-1]
	}
	return KeyValue[K]{Key: n.keys[len(n.keys)-1], Value: v}, true
}

type node[K Key] struct {
	keys     []K
	children []*node[K]
	values   []any
	left     *node[K]
	right    *node[K]
	bmin     int
}

func newInternalNode[K Key](size int) *node[K] {
	return &node[K]{
		keys:     make([]K, 0, size-1),
		children: make([]*node[K], 0, size),
		bmin:     int(math.Ceil(float64(size) / 2)),
	}
}

func newLeafNode[K Key](size int) *node[K] {
	return &node[K]{
		keys:   make([]K, 0, size),
		values: make([]any, 0, size),
		bmin:   int(math.Ceil(float64(size) / 2)),
	}
}

func (n *node[K]) isInternal() bool {
	return n.children != nil
}

func (n *node[K]) isLeaf() bool {
	return n.values != nil
}

func (n *node[K]) insert(key K, val any, replace bool) (ok bool, key2 K, n2 *node[K]) {
	if n.isLeaf() {
		return n.insertToLeaf(key, val, replace)
	}
	for i, c := range n.children {
		if i == len(n.keys) || key < n.keys[i] {
			ok, key2, n2 = c.insert(key, val, replace)
			break
		}
	}
	if n2 != nil {
		key2, n2 = n.insertToInternal(key2, n2)
	}
	return
}

func (n *node[K]) insertToLeaf(key K, val any, replace bool) (ok bool, key2 K, n2 *node[K]) {
	var pos int
	for i, k := range n.keys {
		if k > key {
			break
		}
		if k == key {
			if replace {
				n.values[i] = val
				return false, key2, n2
			} else {
				if c, ok := n.values[i].(collision); !ok {
					c = collision{n.values[i], val}
					n.values[i] = c
				} else {
					n.values[i] = append(c, val)
				}
				return true, key2, n2
			}
		}
		if k < key {
			pos = i + 1
			continue
		}
	}
	if len(n.keys) < cap(n.keys) {
		n.keys = n.keys[:len(n.keys)+1]
		n.values = n.values[:len(n.values)+1]
		copy(n.keys[pos+1:], n.keys[pos:len(n.keys)-1])
		copy(n.values[pos+1:], n.values[pos:len(n.values)-1])
		n.keys[pos] = key
		n.values[pos] = val
		return true, key2, n2
	}
	n2 = newLeafNode[K](cap(n.keys))
	n2.right = n.right
	if n.right != nil {
		n.right.left = n2
	}
	n.right = n2
	n2.left = n
	n2.keys = n2.keys[:cap(n.keys)+1-n.bmin]
	n2.values = n2.values[:cap(n.values)+1-n.bmin]
	if pos < n.bmin {
		copy(n2.keys, n.keys[n.bmin-1:])
		copy(n2.values, n.values[n.bmin-1:])
		n.keys = n.keys[:n.bmin]
		n.values = n.values[:n.bmin]
		copy(n.keys[pos+1:], n.keys[pos:n.bmin-1])
		copy(n.values[pos+1:], n.values[pos:n.bmin-1])
		n.keys[pos] = key
		n.values[pos] = val
	} else {
		pos2 := pos - n.bmin
		copy(n2.keys, n.keys[n.bmin:pos])
		copy(n2.values, n.values[n.bmin:pos])
		n2.keys[pos2] = key
		n2.values[pos2] = val
		copy(n2.keys[pos2+1:], n.keys[pos:])
		copy(n2.values[pos2+1:], n.values[pos:])
		n.keys = n.keys[:n.bmin]
		n.values = n.values[:n.bmin]
	}
	trimValueSlice(n.values)
	return true, n2.keys[0], n2
}

func (n *node[K]) insertToInternal(key K, child *node[K]) (key2 K, n2 *node[K]) {
	var pos int
	for i, k := range n.keys {
		if k < key {
			pos = i + 1
			continue
		}
		break
	}
	cpos := pos + 1
	if len(n.children) < cap(n.children) {
		n.keys = n.keys[:len(n.keys)+1]
		n.children = n.children[:len(n.children)+1]
		copy(n.keys[pos+1:], n.keys[pos:len(n.keys)-1])
		copy(n.children[cpos+1:], n.children[cpos:len(n.children)-1])
		n.keys[pos] = key
		n.children[cpos] = child
		return
	}
	n2 = newInternalNode[K](cap(n.children))
	n2.right = n.right
	if n.right != nil {
		n.right.left = n2
	}
	n.right = n2
	n2.left = n
	n2.keys = n2.keys[:cap(n.keys)+1-n.bmin]
	n2.children = n2.children[:cap(n.children)+1-n.bmin]
	if pos < n.bmin-1 {
		key2 = n.keys[n.bmin-2]
		copy(n2.keys, n.keys[n.bmin-1:])
		copy(n2.children, n.children[n.bmin-1:])
		n.keys = n.keys[:n.bmin-1]
		n.children = n.children[:n.bmin]
		copy(n.keys[pos+1:], n.keys[pos:n.bmin-2])
		copy(n.children[cpos+1:], n.children[cpos:n.bmin-1])
		n.keys[pos] = key
		n.children[cpos] = child
	} else if pos == n.bmin-1 {
		key2 = key
		copy(n2.keys, n.keys[n.bmin-1:])
		copy(n2.children[1:], n.children[n.bmin:])
		n2.children[0] = child
		n.keys = n.keys[:n.bmin-1]
		n.children = n.children[:n.bmin]
	} else { // pos > n.bmin-1
		key2 = n.keys[n.bmin-1]
		pos2, cpos2 := pos-n.bmin, cpos-n.bmin
		copy(n2.keys, n.keys[n.bmin:pos])
		copy(n2.children, n.children[n.bmin:cpos])
		n2.keys[pos2] = key
		n2.children[cpos2] = child
		copy(n2.keys[pos2+1:], n.keys[pos:])
		copy(n2.children[cpos2+1:], n.children[cpos:])
		n.keys = n.keys[:n.bmin-1]
		n.children = n.children[:n.bmin]
	}
	trimNodeSlice(n.children)
	return
}

func (n *node[K]) delete(key K, all bool, idx int) (val any, ok bool) {
	if n.isLeaf() {
		return n.deleteFromLeaf(key, all, idx)
	}
	var i int
	var c *node[K]
	for i, c = range n.children {
		if i == len(n.keys) || key < n.keys[i] {
			val, ok = c.delete(key, all, idx)
			break
		}
	}
	if ok {
		if c.isLeaf() {
			if len(c.values) < n.bmin {
				n.balanceLeaf(i)
			}
		} else {
			if len(c.children) < n.bmin {
				n.balanceInternal(i)
			}
		}
	}
	return
}

func (n *node[K]) deleteFromLeaf(key K, all bool, idx int) (val any, ok bool) {
	for i, k := range n.keys {
		if k == key {
			if all {
				if c, ok := n.values[i].(collision); !ok {
					val = collision{n.values[i]}
				} else {
					val = c
				}
			} else {
				if c, ok := n.values[i].(collision); !ok {
					if idx > 0 {
						return nil, false
					}
					val = n.values[i]
				} else {
					if idx >= len(c) {
						return nil, false
					}
					if idx < 0 {
						val = c[len(c)-1]
						c[len(c)-1] = nil
						n.values[i] = c[:len(c)-1]
					} else {
						val = c[idx]
						copy(c[idx:], c[idx+1:])
						c[len(c)-1] = nil
						n.values[i] = c[:len(c)-1]
					}
					if len(n.values[i].(collision)) != 0 {
						return val, true
					}
				}
			}
			ok = true
			copy(n.keys[i:len(n.keys)-1], n.keys[i+1:len(n.keys)])
			copy(n.values[i:len(n.values)-1], n.values[i+1:len(n.values)])
			n.keys = n.keys[:len(n.keys)-1]
			n.values[len(n.values)-1] = nil
			n.values = n.values[:len(n.values)-1]
			return
		}
	}
	return
}

func (n *node[K]) balanceLeaf(i int) {
	c := n.children[i]
	if i != 0 && len(n.children[i-1].values) > n.bmin {
		n.keys[i-1] = c.takeFromLeftSiblingLeaf(n.children[i-1])
		return
	}
	if i != len(n.children)-1 && len(n.children[i+1].values) > n.bmin {
		n.keys[i] = c.takeFromRightSiblingLeaf(n.children[i+1])
		return
	}
	if i != 0 && (i == len(n.children)-1 || len(n.children[i-1].values) < len(n.children[i+1].values)) {
		mergeLeafs(n.children[i-1], c)
		n.deleteChild(i)
	} else {
		mergeLeafs(c, n.children[i+1])
		n.deleteChild(i + 1)
	}
}

func (n *node[K]) takeFromLeftSiblingLeaf(n2 *node[K]) K {
	n.keys = n.keys[:len(n.keys)+1]
	copy(n.keys[1:], n.keys[:len(n.keys)-1])
	n.keys[0] = n2.keys[len(n2.keys)-1]
	n2.keys = n2.keys[:len(n2.keys)-1]
	n.values = n.values[:len(n.values)+1]
	copy(n.values[1:], n.values[:len(n.values)-1])
	n.values[0] = n2.values[len(n2.values)-1]
	n2.values[len(n2.values)-1] = nil
	n2.values = n2.values[:len(n2.values)-1]
	return n.keys[0]
}

func (n *node[K]) takeFromRightSiblingLeaf(n2 *node[K]) K {
	n.keys = n.keys[:len(n.keys)+1]
	n.keys[len(n.keys)-1] = n2.keys[0]
	copy(n2.keys[:len(n2.keys)-1], n2.keys[1:len(n2.keys)])
	n2.keys = n2.keys[:len(n2.keys)-1]
	n.values = n.values[:len(n.values)+1]
	n.values[len(n.values)-1] = n2.values[0]
	copy(n2.values[:len(n2.values)-1], n2.values[1:len(n2.values)])
	n2.values[len(n2.values)-1] = nil
	n2.values = n2.values[:len(n2.values)-1]
	return n2.keys[0]
}

func (n *node[K]) balanceInternal(i int) {
	c := n.children[i]
	if i != 0 && len(n.children[i-1].children) > n.bmin {
		n.keys[i-1] = c.takeFromLeftSiblingInternal(n.children[i-1], n.keys[i-1])
		return
	}
	if i != len(n.children)-1 && len(n.children[i+1].children) > n.bmin {
		n.keys[i] = c.takeFromRightSiblingInternal(n.children[i+1], n.keys[i])
		return
	}
	if i != 0 && (i == len(n.children)-1 || len(n.children[i-1].children) < len(n.children[i+1].children)) {
		mergeInternal(n.children[i-1], c, n.keys[i-1])
		n.deleteChild(i)
	} else {
		mergeInternal(c, n.children[i+1], n.keys[i])
		n.deleteChild(i + 1)
	}
}

func (n *node[K]) takeFromLeftSiblingInternal(n2 *node[K], key K) K {
	n.keys = n.keys[:len(n.keys)+1]
	copy(n.keys[1:], n.keys[:len(n.keys)-1])
	mkey := n2.keys[len(n2.keys)-1]
	n.keys[0] = key
	n2.keys = n2.keys[:len(n2.keys)-1]
	n.children = n.children[:len(n.children)+1]
	copy(n.children[1:], n.children[:len(n.children)-1])
	n.children[0] = n2.children[len(n2.children)-1]
	n2.children[len(n2.children)-1] = nil
	n2.children = n2.children[:len(n2.children)-1]
	return mkey
}

func (n *node[K]) takeFromRightSiblingInternal(n2 *node[K], key K) K {
	n.keys = n.keys[:len(n.keys)+1]
	n.keys[len(n.keys)-1] = key
	mkey := n2.keys[0]
	copy(n2.keys[:len(n2.keys)-1], n2.keys[1:len(n2.keys)])
	n2.keys = n2.keys[:len(n2.keys)-1]
	n.children = n.children[:len(n.children)+1]
	n.children[len(n.children)-1] = n2.children[0]
	copy(n2.children[:len(n2.children)-1], n2.children[1:len(n2.children)])
	n2.children[len(n2.children)-1] = nil
	n2.children = n2.children[:len(n2.children)-1]
	return mkey
}

func (n *node[K]) deleteChild(i int) {
	copy(n.keys[i-1:len(n.keys)-1], n.keys[i:len(n.keys)])
	n.keys = n.keys[:len(n.keys)-1]
	copy(n.children[i:len(n.children)-1], n.children[i+1:len(n.children)])
	n.children[len(n.children)-1] = nil
	n.children = n.children[:len(n.children)-1]
}

func mergeLeafs[K Key](l, r *node[K]) {
	l.right = r.right
	if r.right != nil {
		r.right.left = l
	}
	llen, rlen := len(l.keys), len(r.keys)
	l.keys = l.keys[:llen+rlen]
	copy(l.keys[llen:], r.keys)
	l.values = l.values[:llen+rlen]
	copy(l.values[llen:], r.values)
}

func mergeInternal[K Key](l, r *node[K], key K) {
	l.right = r.right
	if r.right != nil {
		r.right.left = l
	}
	nlkeys, nlch := len(l.keys), len(l.children)
	l.keys = l.keys[:nlkeys+len(r.keys)+1]
	l.keys[nlkeys] = key
	copy(l.keys[nlkeys+1:], r.keys)
	l.children = l.children[:len(l.keys)+1]
	copy(l.children[nlch:], r.children)
}

func trimNodeSlice[K Key](s []*node[K]) {
	s = s[len(s):cap(s)]
	if len(s) == 0 {
		return
	}
	s[0] = nil
	for i := 1; i < len(s); i *= 2 {
		copy(s[i:], s[:i])
	}
}

func trimValueSlice(s []any) {
	s = s[len(s):cap(s)]
	if len(s) == 0 {
		return
	}
	s[0] = nil
	for i := 1; i < len(s); i *= 2 {
		copy(s[i:], s[:i])
	}
}
