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

package bptree

import (
	"fmt"
	"math/rand"
	"runtime"
	"sort"
	"testing"
	"time"
)

const (
	bmax               = 32
	numKeys            = 1000
	benchBmax          = 128
	benchNumKeys       = 1000000
	numRangeTestKeys   = 30
	numExtraKeys       = 10
	leakTestNumKeys    = 1000000
	leakTestIterations = 100
	leakTestValueSize  = 7000
)

func fail[K Key](T *testing.T, t *BPTree[K], args ...any) {
	fmt.Println()
	printTree(t)
	T.Fatal(args...)
}

func failf[K Key](T *testing.T, t *BPTree[K], format string, args ...any) {
	fail(T, t, fmt.Errorf(format, args...))
}

func printTree[K Key](t *BPTree[K]) {
	var printNode func(n *node[K], label string)
	printNode = func(n *node[K], label string) {
		content := ""
		for i, k := range n.keys {
			if i != 0 {
				content += " "
			}
			if n.isLeaf() {
				if v, ok := n.values[i].(collision); ok {
					content += fmt.Sprintf("(%v: ", k)
					for i, v := range v {
						if i != 0 {
							content += ", "
						}
						content += fmt.Sprint(v)
					}
					content += ")"
				} else {
					content += fmt.Sprintf("(%v: %v)", k, n.values[i])
				}
			} else {
				content += fmt.Sprintf("[%v]", k)
			}
		}
		fmt.Printf("%.15s: %s\n", label, content)
		for i, c := range n.children {
			l := label + "-"
			if i < len(n.keys) {
				l += fmt.Sprint(n.keys[i])
			} else {
				l += ">"
			}
			printNode(c, l)
		}
	}
	printNode(t.root, "root")
}

func validateTree[K Key](t *BPTree[K]) error {
	maxDepth, numVisited, numOnLevels := -1, 0, 0
	var visitNode func(n *node[K], min, max *K, depth int) error
	visitNode = func(n *node[K], min, max *K, depth int) error {
		numVisited++
		if n.isLeaf() {
			if maxDepth == -1 {
				maxDepth = depth
			} else if maxDepth != depth {
				return fmt.Errorf("maxDepth(%d) != depth(%d)", maxDepth, depth)
			}
			if len(n.keys) != len(n.values) {
				return fmt.Errorf("len(leaf.keys)(%d) != len(leaf.values)(%d)", len(n.keys), len(n.values))
			}
			if depth != 0 && len(n.keys) < n.bmin {
				return fmt.Errorf("len(leaf.keys)(%d) < bmin(%d)", len(n.keys), n.bmin)
			}
			if depth != 0 {
				for _, k := range n.keys {
					if min != nil && k < *min {
						return fmt.Errorf("leaf.key(%v) < min(%v)", k, *min)
					} else if max != nil && k >= *max {
						return fmt.Errorf("leaf.key(%v) >= max(%v)", k, *max)
					}
				}
			}
		} else {
			if len(n.keys) != len(n.children)-1 {
				return fmt.Errorf("len(node.keys)(%d) != len(node.children)-1(%d)", len(n.keys), len(n.children)-1)
			}
			if depth != 0 && len(n.children) < n.bmin {
				return fmt.Errorf("len(node.children)(%d) < bmin(%d)", len(n.children), n.bmin)
			}
			for i, c := range n.children {
				if i < len(n.keys) {
					if min != nil && n.keys[i] < *min {
						return fmt.Errorf("node.key(%v) < min(%v)", n.keys[i], *min)
					} else if max != nil && n.keys[i] >= *max {
						return fmt.Errorf("node.key(%v) >= max(%v)", n.keys[i], *max)
					}
				}
				var cmin, cmax *K
				if i == 0 {
					cmin = min
					if len(n.keys) == 0 {
						cmax = max
					} else {
						cmax = &(n.keys[0])
					}
				} else if i == len(n.keys) {
					cmin, cmax = &(n.keys[i-1]), max
				} else {
					cmin, cmax = &(n.keys[i-1]), &(n.keys[i])
				}
				if err := visitNode(c, cmin, cmax, depth+1); err != nil {
					return err
				}
			}
		}
		return nil
	}
	var checkLevelLinks func(lvl int) error
	checkLevelLinks = func(lvl int) error {
		var nodes []*node[K]
		var getLevelNodes func(n *node[K], depth int) error
		getLevelNodes = func(n *node[K], depth int) error {
			if depth == lvl {
				nodes = append(nodes, n)
				return nil
			}
			if n.isLeaf() {
				return fmt.Errorf("maxDepth(%d) != depth(%d)", maxDepth, depth)
			}
			for _, c := range n.children {
				if err := getLevelNodes(c, depth+1); err != nil {
					return err
				}
			}
			return nil
		}
		if err := getLevelNodes(t.root, 0); err != nil {
			return err
		}
		numOnLevels += len(nodes)
		if len(nodes) == 0 {
			return fmt.Errorf("empty level(%d)", lvl)
		}
		for i, n := range nodes {
			if i == 0 && n.left != nil {
				return fmt.Errorf("first.left != nil on level(%d)", lvl)
			}
			if i != 0 && n.left != nodes[i-1] {
				return fmt.Errorf("node.left != previous on level(%d)", lvl)
			}
			if i == len(nodes)-1 && n.right != nil {
				return fmt.Errorf("last.right != nil on level(%d)", lvl)
			}
			if i != len(nodes)-1 && n.right != nodes[i+1] {
				return fmt.Errorf("node.right != next on level(%d)", lvl)
			}
		}
		return nil
	}
	if err := visitNode(t.root, nil, nil, 0); err != nil {
		return err
	}
	for lvl := 0; lvl <= maxDepth; lvl++ {
		if err := checkLevelLinks(lvl); err != nil {
			return err
		}
	}
	if numVisited != numOnLevels {
		return fmt.Errorf("numVisited(%d) != numOnLevels(%d)", numVisited, numOnLevels)
	}
	return nil
}

func isEmpty[K Key](t *BPTree[K]) bool {
	return t.root.isLeaf() && len(t.root.keys) == 0 && len(t.root.values) == 0
}

func valueForKey[K Key](key K) string       { return fmt.Sprintf("v_%v", key) }
func leakTestValueForKey[K Key](_ K) []byte { return make([]byte, leakTestValueSize) }

func genKeys(n int) []int {
	keys := make([]int, n)
	for i := 0; i < n; i++ {
		keys[i] = i
	}
	shuffleKeys(keys)
	return keys
}

func genExtraKeys(n, ne int) ([]int, []*int) {
	keys := make([]int, n)
	extra := make([]*int, n+ne)
	j := 0
	for i := 1; i < len(extra); i++ {
		e := i
		extra[i] = &e
		if i >= ne/2 && j < len(keys) {
			keys[j] = i
			j++
		}
	}
	extra[0] = nil
	shuffleKeys(keys)
	return keys, extra
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func shuffleKeys(keys []int) {
	rand.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
}

func validateInsert[K Key](T *testing.T, t *BPTree[K], keys []K, i int) {
	if err := validateTree(t); err != nil {
		failf(T, t, "tree validation failed: %s", err)
	}
	for j := 0; j <= i; j++ {
		k := keys[j]
		v, ok := t.Find(k)
		if !ok {
			failf(T, t, "key not found: %v", k)
		}
		if v != valueForKey(k) {
			failf(T, t, "value differs: found: %s, needed: %s", v, valueForKey(k))
		}
	}
}

func TestInsert(T *testing.T) {
	t := NewBPTree[int](bmax)
	keys := genKeys(numKeys)
	keys = append(keys, keys...)
	shuffleKeys(keys)
	inserted := make(map[int]struct{})
	fmt.Println("inserting...")
	for i, k := range keys {
		if i != 0 {
			fmt.Print(", ")
		}
		fmt.Print(k)
		t.Insert(k, valueForKey(k))
		inserted[k] = struct{}{}
		if t.Size() != len(inserted) {
			failf(T, t, "invalid size: %d, must be %d", t.Size(), len(inserted))
		}
		validateInsert(T, t, keys, i)
	}
	fmt.Println()
}

func makeAppendKeysValues(n int) ([]int, []int) {
	keys := genKeys(n)
	keys = append(append(append(append(append([]int{}, keys...), keys...), keys...), keys...), keys...)
	values := genKeys(5*n)
	shuffleKeys(keys)
	shuffleKeys(values)
	return keys, values
}

func makeTreeAppendWithKeysValues(T *testing.T, b int, keys, values []int) ([]int, []int, *BPTree[int], map[int][]int) {
	t := NewBPTree[int](b)
	fmt.Println("appending...")
	m := make(map[int][]int)
	for i, k := range keys {
		if i != 0 {
			fmt.Print(", ")
		}
		fmt.Print(k)
		t.Append(k, values[i])
		if t.Size() != i + 1 {
			failf(T, t, "invalid size: %d, must be %d", t.Size(), i+1)
		}
		if v, ok := m[k]; !ok {
			m[k] = []int{values[i]}
		} else {
			m[k] = append(v, values[i])
		}
		validateAppend(T, t, keys, values, i)
	}
	fmt.Println()
	return keys, values, t, m
}

func makeTreeAppend(T *testing.T, b, n int) ([]int, []int, *BPTree[int], map[int][]int) {
	keys, values := makeAppendKeysValues(n)
	return makeTreeAppendWithKeysValues(T, b, keys, values)
}

func compareWithMap(T *testing.T, t *BPTree[int], m map[int][]int) {
	if err := validateTree(t); err != nil {
		failf(T, t, "tree validation failed: %s", err)
	}
	size := 0
	for k, mv := range m {
		size += len(mv)
		tv, ok := t.FindAll(k)
		if !ok {
			failf(T, t, "key not found: %d", k)
		}
		if len(tv) != len(mv) {
			failf(T, t, "value count differs: found %d, needed %d", len(tv), len(mv))
		}
		for i, tv := range tv {
			mv := mv[i]
			if tv != mv {
				failf(T, t, "value differs: found %d, needed %d", tv, mv)
			}
		}
	}
	if t.Size() != size {
		failf(T, t, "size differs: found %d, needed %d", t.Size(), size)
	}
}

func validateAppend[K Key](T *testing.T, t *BPTree[K], keys []K, values []int, i int) {
	if err := validateTree(t); err != nil {
		failf(T, t, "tree validation failed: %s", err)
	}
	duplicates := make(map[K]int)
	for j := 0; j <= i; j++ {
		k := keys[j]
		v, ok := t.FindAll(k)
		if !ok {
			failf(T, t, "key not found: %v", k)
		}
		duplicates[k]++
		if len(v) < duplicates[k] {
			failf(T, t, "number of keys differs: found %d, needed %d", len(v), duplicates[k])
		}
		if v[duplicates[k]-1] != values[j] {
			failf(T, t, "value differs: found: %d, needed: %d", v[duplicates[k]-1], values[j])
		}
	}
}

func TestInsertAppend(T *testing.T) {
	b, n := bmax, numKeys
	makeTreeAppend(T, b, n)
}

func validateDelete[K Key](T *testing.T, t *BPTree[K], keys []K, i int) {
	if v, ok := t.Find(keys[i]); ok {
		failf(T, t, "found after delete: %s", v)
	}
	if err := validateTree(t); err != nil {
		failf(T, t, "tree validation failed: %s", err)
	}
}

func TestDelete(T *testing.T) {
	t := NewBPTree[int](bmax)
	keys := genKeys(numKeys)
	keys = append(keys, keys...)
	shuffleKeys(keys)
	inserted := make(map[int]struct{})
	fmt.Println("inserting...")
	for i, k := range keys {
		if i != 0 {
			fmt.Print(", ")
		}
		fmt.Print(k)
		t.Insert(k, valueForKey(k))
		inserted[k] = struct{}{}
		if t.Size() != len(inserted) {
			failf(T, t, "invalid size: %d, must be %d", t.Size(), len(inserted))
		}
	}
	fmt.Println()
	shuffleKeys(keys)
	fmt.Println("deleting...")
	for i, k := range keys {
		if i != 0 {
			fmt.Print(", ")
		}
		fmt.Print(k)
		if v, ok := t.Delete(k); !ok {
			if _, ok = inserted[k]; ok {
				failf(T, t, "deleting failed: %d", k)
			}
		} else if v != valueForKey(k) {
			failf(T, t, "deleted wrong value: %s, needed: %s", v.(string), valueForKey(k))
		}
		delete(inserted, k)
		validateDelete(T, t, keys, i)
		if t.Size() != len(inserted) {
			failf(T, t, "invalid size: %d, must be %d", t.Size(), len(inserted))
		}
	}
	if !isEmpty(t) {
		fail(T, t, "tree is not empty")
	}
	fmt.Println()
}

func TestDeleteAppend(T *testing.T) {
	b, n := bmax, numKeys
	keys, _, t, m := makeTreeAppend(T, b, n)
	shuffleKeys(keys)
	for _, k := range keys {
		mv, ok := m[k]
		if !ok {
			failf(T, t, "key %d not found in comparation map", k)
		}
		m[k] = mv[:len(mv)-1]
		t.Delete(k)
		compareWithMap(T, t, m)
	}
	keys, _, t, m = makeTreeAppend(T, b, n)
	shuffleKeys(keys)
	for _, k := range keys {
		mv, ok := m[k]
		if !ok {
			failf(T, t, "key %d not found in comparation map", k)
		}
		idx := rand.Intn(len(mv))
		copy(mv[idx:], mv[idx+1:])
		m[k] = mv[:len(mv)-1]
		t.DeleteOne(k, idx)
		compareWithMap(T, t, m)
	}
	keys, _, t, m = makeTreeAppend(T, b, n)
	shuffleKeys(keys)
	for _, k := range keys {
		delete(m, k)
		t.DeleteAll(k)
		compareWithMap(T, t, m)
	}
}


func TestFirstLast(T *testing.T) {
	t := NewBPTree[int](bmax)
	keys := genKeys(numKeys)
	var min, max = numKeys, -1
	for i, k := range keys {
		if i == 0 {
			if _, ok := t.First(); ok {
				fail(T, t, "first found when tree is empty")
			}
			if _, ok := t.Last(); ok {
				fail(T, t, "last found when tree is empty")
			}
		}
		t.Insert(k, valueForKey(k))
		if k < min {
			min = k
		}
		if k > max {
			max = k
		}
		f, ok := t.First()
		if !ok {
			fail(T, t, "first not found")
		} else if f.Key != min {
			failf(T, t, "first.Key(%d) != min(%d)", f.Key, min)
		}
		l, ok := t.Last()
		if !ok {
			fail(T, t, "last not found")
		} else if l.Key != max {
			failf(T, t, "last.Key(%d) != max(%d)", f.Key, max)
		}
	}
}

func TestFirstLastAppend(T *testing.T) {
	b, n := bmax, numKeys
	t := NewBPTree[int](b)
	keys, values := makeAppendKeysValues(n)
	var min, max = n, -1
	for i, k := range keys {
		if i == 0 {
			if _, ok := t.First(); ok {
				fail(T, t, "first found when tree is empty")
			}
			if _, ok := t.Last(); ok {
				fail(T, t, "last found when tree is empty")
			}
		}
		t.Append(k, values[i])
		if k < min {
			min = k
		}
		if k > max {
			max = k
		}
		f, ok := t.First()
		if !ok {
			fail(T, t, "first not found")
		} else if f.Key != min {
			failf(T, t, "first.Key(%d) != min(%d)", f.Key, min)
		}
		l, ok := t.Last()
		if !ok {
			fail(T, t, "last not found")
		} else if l.Key != max {
			failf(T, t, "last.Key(%d) != max(%d)", f.Key, max)
		}
	}
}

func TestRange(T *testing.T) {
	b, n, ne := bmax, numRangeTestKeys, numExtraKeys
	t := NewBPTree[int](b)
	keys, extraKeys := genExtraKeys(n, ne)
	fmt.Println("inserting...")
	for _, k := range keys {
		t.Insert(k, valueForKey(k))
	}
	sort.Ints(keys)
	fmt.Println(keys)
	for i, k := range extraKeys {
		if i != 0 {
			fmt.Print(", ")
		}
		if k == nil {
			fmt.Print("nil")
		} else {
			fmt.Print(*k)
		}
	}
	fmt.Println()
	for i, from := range extraKeys {
		for j, to := range extraKeys {
			treeRange := t.Range(from, to)
			var keysRange []int
			keysFrom := i - ne/2
			if from == nil || keysFrom < 0 {
				keysFrom = 0
			}
			keysTo := j - ne/2
			if to == nil || keysTo > len(keys) {
				keysTo = len(keys)
			}
			if keysFrom <= keysTo && keysFrom < len(keys) && keysTo >= 0 {
				keysRange = keys[keysFrom:keysTo]
			}
			if len(keysRange) != len(treeRange) {
				T.Fatalf("invalid len(range): len[%v:%v](%v) != len[%v:%v](%v)", i, j, len(treeRange), keysFrom, keysTo, len(keysRange))
			}
			for i, key := range keysRange {
				if key != treeRange[i].Key {
					T.Fatalf("treeRange[i].Key != key")
				}
			}
			var printFrom, printTo string
			if from == nil {
				printFrom = "nil"
			} else {
				printFrom = fmt.Sprint(*from)
			}
			if to == nil {
				printTo = "nil"
			} else {
				printTo = fmt.Sprint(*to)
			}
			fmt.Printf("Range(%s,%s) = ", printFrom, printTo)
			if treeRange == nil {
				fmt.Println("nil")
			} else {
				fmt.Print("[")
				for i, kv := range treeRange {
					if i != 0 {
						fmt.Print(", ")
					}
					fmt.Print(kv.Key)
				}
				fmt.Println("]")
			}
		}
	}
}

func TestRangeAppend(T *testing.T) {
	b, n, ne := bmax, numRangeTestKeys, numExtraKeys
	_, values := makeAppendKeysValues(n)
	keys, extraKeys := genExtraKeys(n, ne)
	_, _, t, m := makeTreeAppendWithKeysValues(T, b, keys, values)
	sort.Ints(keys)
	fmt.Println(keys)
	for i, k := range extraKeys {
		if i != 0 {
			fmt.Print(", ")
		}
		if k == nil {
			fmt.Print("nil")
		} else {
			fmt.Print(*k)
		}
	}
	fmt.Println()
	for i, from := range extraKeys {
		for j, to := range extraKeys {
			treeRange := t.Range(from, to)
			var mapRange []int
			keysFrom := i - ne/2
			if from == nil || keysFrom < 0 {
				keysFrom = 0
			}
			keysTo := j - ne/2
			if to == nil || keysTo > len(keys) {
				keysTo = len(keys)
			}
			for k := keysFrom; k < keysTo; k++ {
				if v, ok := m[keys[k]]; ok {
					mapRange = append(mapRange, v...)
				}
			}
			if len(mapRange) != len(treeRange) {
				T.Fatalf("invalid len(range): len[%v:%v](%v) != len[%v:%v](%v)", i, j, len(treeRange), keysFrom, keysTo, len(mapRange))
			}
			for i, v := range mapRange {
				if v != treeRange[i].Value {
					T.Fatalf("mapRange[i] (%d) != treeRange[i].Value (%d)", v, treeRange[i].Value)
				}
			}
			var printFrom, printTo string
			if from == nil {
				printFrom = "nil"
			} else {
				printFrom = fmt.Sprint(*from)
			}
			if to == nil {
				printTo = "nil"
			} else {
				printTo = fmt.Sprint(*to)
			}
			fmt.Printf("Range(%s,%s) = ", printFrom, printTo)
			if treeRange == nil {
				fmt.Println("nil")
			} else {
				fmt.Print("[")
				for i, kv := range treeRange {
					if i != 0 {
						fmt.Print(", ")
					}
					fmt.Print(kv.Key)
				}
				fmt.Println("]")
			}
		}
	}
}

func printMemStats(msg string, old *runtime.MemStats) *runtime.MemStats {
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	fmt.Printf(
		"--------------------\nMemory stats: %s\nAlloc: %d\nTotalAlloc: %d\nSys: %d\nMallocs: %d\nFrees: %d\nLiveObjects: %d\n",
		msg,
		ms.Alloc,
		ms.TotalAlloc,
		ms.Sys,
		ms.Mallocs,
		ms.Frees,
		ms.Mallocs-ms.Frees,
	)
	if old != nil {
		fmt.Println("New objects by size:")
		for i, s := range ms.BySize {
			if delta := int64(s.Mallocs-s.Frees) - int64(old.BySize[i].Mallocs-old.BySize[i].Frees); delta > 0 {
				fmt.Printf("%d: %d\n", s.Size, delta)
			}
		}
	}
	fmt.Println("--------------------")
	return &ms
}

func _TestMemoryLeak(T *testing.T) {
	t := NewBPTree[int](bmax)
	ms := printMemStats("start", nil)
	for i := 0; i < leakTestIterations; i++ {
		fmt.Println("iteration", i)
		keys := genKeys(leakTestNumKeys)
		for _, k := range keys {
			t.Insert(k, leakTestValueForKey(k))
		}
		shuffleKeys(keys)
		for _, k := range keys {
			t.Delete(k)
		}
		runtime.GC()
	}
	printMemStats("all deleted", ms)
}

func _TestDebug(T *testing.T) {
	//b := bmax
	b := 4
	var appendOrder = []int{0, 3, 3, 0, 6, 9, 6, 6}
	var values = []int{6, 10, 23, 33, 40, 56, 69, 76}
	t := NewBPTree[int](b)
	keys := appendOrder
	fmt.Println("appending...")
	for i, k := range appendOrder {
		//if i != 0 {
		//	fmt.Print(", ")
		//}
		//fmt.Print(k)
		t.Append(k, values[i])
		printTree(t)
		validateAppend(T, t, keys, values, i)
	}
	fmt.Println()
}

func BenchmarkBPTreeInsert(b *testing.B) {
	t := NewBPTree[int](benchBmax)
	keys := genKeys(benchNumKeys)
	b.ResetTimer()
	for _, k := range keys {
		t.Insert(k, valueForKey(k))
	}
}

func BenchmarkMapInsert(b *testing.B) {
	m := make(map[int]any)
	keys := genKeys(benchNumKeys)
	b.ResetTimer()
	for _, k := range keys {
		m[k] = valueForKey(k)
	}
}

func BenchmarkAllocatedMapInsert(b *testing.B) {
	m := make(map[int]any, benchNumKeys)
	keys := genKeys(benchNumKeys)
	b.ResetTimer()
	for _, k := range keys {
		m[k] = valueForKey(k)
	}
}

func BenchmarkBPTreeFind(b *testing.B) {
	t := NewBPTree[int](benchBmax)
	keys := genKeys(benchNumKeys)
	for _, k := range keys {
		t.Insert(k, valueForKey(k))
	}
	shuffleKeys(keys)
	b.ResetTimer()
	for _, k := range keys {
		_, _ = t.Find(k)
	}
}

func BenchmarkMapFind(b *testing.B) {
	m := make(map[int]any)
	keys := genKeys(benchNumKeys)
	for _, k := range keys {
		m[k] = valueForKey(k)
	}
	shuffleKeys(keys)
	b.ResetTimer()
	for _, k := range keys {
		_, _ = m[k]
	}
}

func BenchmarkAllocatedMapFind(b *testing.B) {
	m := make(map[int]any, benchNumKeys)
	keys := genKeys(benchNumKeys)
	for _, k := range keys {
		m[k] = valueForKey(k)
	}
	shuffleKeys(keys)
	b.ResetTimer()
	for _, k := range keys {
		_, _ = m[k]
	}
}

