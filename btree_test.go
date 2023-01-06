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
	"testing"
	"time"
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
				content += fmt.Sprintf("(%v)", k)
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

func shuffleKeys(keys []int) {
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
}

const (
	bmax                 = 3
	numKeys              = 1000
	leakTestNumKeys      = 1000000
	leakTestNumFixedKeys = leakTestNumKeys / 20
	leakTestIterations   = 10
	leakTestValueSize    = 7000
)

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
	t, err := NewBPTree[int](bmax)
	if err != nil {
		T.Fatal(err)
	}
	keys := genKeys(numKeys)
	fmt.Println("inserting...")
	for i, k := range keys {
		if i != 0 {
			fmt.Print(", ")
		}
		fmt.Print(k)
		t.Insert(k, valueForKey(k))
		validateInsert(T, t, keys, i)
	}
	fmt.Println()
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
	t, err := NewBPTree[int](bmax)
	if err != nil {
		T.Fatal(err)
	}
	keys := genKeys(numKeys)
	fmt.Println("inserting...")
	for i, k := range keys {
		if i != 0 {
			fmt.Print(", ")
		}
		fmt.Print(k)
		t.Insert(k, valueForKey(k))
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
			failf(T, t, "deleting failed: %d", k)
		} else if v != valueForKey(k) {
			failf(T, t, "deleted wrong value: %s, needed: %s", v.(string), valueForKey(k))
		}
		validateDelete(T, t, keys, i)
	}
	if !isEmpty(t) {
		fail(T, t, "tree is not empty")
	}
	fmt.Println()
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

func TestMemoryLeak(T *testing.T) {
	t, err := NewBPTree[int](bmax)
	if err != nil {
		T.Fatal(err)
	}
	keys := genKeys(leakTestNumKeys)
	ms := printMemStats("before insert", nil)
	fmt.Println("inserting...")
	for _, k := range keys {
		t.Insert(k, leakTestValueForKey(k))
	}
	printMemStats("after insert", ms)

	for i := 0; i < leakTestIterations; i++ {
		shuffleKeys(keys)
		fmt.Println("iteration", i)
		keys := keys[leakTestNumFixedKeys:]
		for i, k := range keys {
			if i == len(keys)/2 {
				printMemStats("after half delete", ms)
			}
			t.Delete(k)
		}
		printMemStats("after delete", ms)
		shuffleKeys(keys)
		for _, k := range keys {
			t.Insert(k, leakTestValueForKey(k))
		}
		printMemStats("after reinsert", ms)
	}
	printMemStats("all deleted", ms)
}

func TestDebug(T *testing.T) {
	var insertOrder = []int{21, 3, 26, 7, 29, 5, 2, 28, 4, 27, 9, 23, 15, 12, 1, 14, 25, 24, 6, 13, 17, 8, 11, 10, 19, 18, 22, 16, 0, 20}
	var deleteOrder = []int{18, 15, 19, 7, 23, 13, 0, 26}
	t, err := NewBPTree[int](bmax)
	if err != nil {
		T.Fatal(err)
	}
	keys := insertOrder
	fmt.Println("inserting...")
	for i, k := range keys {
		if i != 0 {
			fmt.Print(", ")
		}
		fmt.Print(k)
		t.Insert(k, valueForKey(k))
		validateInsert(T, t, keys, i)
	}
	fmt.Println()
	printTree(t)
	keys = deleteOrder
	fmt.Println("deleting...")
	for i, k := range keys[:len(keys)-1] {
		if i != 0 {
			fmt.Print(", ")
		}
		fmt.Print(k)
		if v, ok := t.Delete(k); !ok {
			failf(T, t, "deleting failed: %d", k)
		} else if v != valueForKey(k) {
			failf(T, t, "deleted wrong value: %s, needed: %s", v.(string), valueForKey(k))
		}
		validateDelete(T, t, keys, i)
	}
	fmt.Println()
	printTree(t)
	k := keys[len(keys)-1]
	fmt.Print(k)
	if v, ok := t.Delete(k); !ok {
		failf(T, t, "deleting failed: %d", k)
	} else if v != valueForKey(k) {
		failf(T, t, "deleted wrong value: %s, needed: %s", v.(string), valueForKey(k))
	}
	validateDelete(T, t, keys, len(keys)-1)
	fmt.Println()
}
