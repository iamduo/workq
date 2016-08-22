package skiplist

import (
	"math/rand"
	"sort"
	"testing"
)

func TestPop(t *testing.T) {
	l := New(compareIntAsc)
	a := 1
	b := 2

	if ok := l.Insert(b); !ok {
		t.Fatalf("Insert failed")
	}
	if ok := l.Insert(a); !ok {
		t.Fatalf("Insert failed")
	}
	if l.Len() != 2 {
		t.Fatalf("Length mismatch, exp=%d, act=%d", 2, l.Len())
	}

	item, ok := l.Pop()
	if !ok {
		t.Fatalf("Pop failed")
	}

	if l.Len() != 1 {
		t.Fatalf("Length mismatch, exp=%d, act=%d", 1, l.Len())
	}

	value := item.(int)
	if value != a {
		t.Fatalf("value=%v != compare=%v", value, a)
	}

	item, ok = l.Pop()
	if !ok {
		t.Fatalf("Pop failed")
	}

	if l.Len() != 0 {
		t.Fatalf("Length mismatch, exp=%d, act=%d", 0, l.Len())
	}

	value = item.(int)
	if value != b {
		t.Fatalf("value=%v != compare=%v", value, b)
	}

	if _, ok := l.Pop(); ok {
		t.Fatalf("Expected list to be empty")
	}
}

func TestInsertDuplicate(t *testing.T) {
	l := New(compareIntAsc)
	a := 1

	if ok := l.Insert(a); !ok {
		t.Fatalf("Insert failed")
	}

	if l.Len() != 1 {
		t.Fatalf("Length mismatch, exp=%d, act=%d", 1, l.Len())
	}

	if ok := l.Insert(a); ok {
		t.Fatalf("Duplicate insert should fail")
	}

	if l.Len() != 1 {
		t.Fatalf("Length mismatch, exp=%d, act=%d", 1, l.Len())
	}
}

func TestInsertAndDelete(t *testing.T) {
	type testInput struct {
		items []int
		asc   bool
	}
	tests := []testInput{
		{[]int{1}, true},
		{[]int{1}, false},

		{[]int{1, 2}, true},
		{[]int{1, 2}, false},

		{[]int{2, 1}, true},
		{[]int{2, 1}, false},

		{[]int{2, 1, 3}, true},
		{[]int{2, 1, 3}, false},
	}

	items := make([]int, 256)
	for i := 0; i < len(items); i++ {
		items[i] = i
	}
	tests = append(tests, testInput{items, true})
	tests = append(tests, testInput{items, false})

	items = rand.Perm(4096)
	tests = append(tests, testInput{items, true})
	tests = append(tests, testInput{items, false})

	for _, tt := range tests {
		leveledUp := false
		for {
			l := New(compareIntAsc)
			if !tt.asc {
				l = New(compareIntDesc)
			}

			x := l.head
			for k, v := range tt.items {
				if !l.Insert(v) {
					t.Fatalf("Insert failed")
				}

				if x.prev != nil || x.item != nil {
					t.Fatalf("Invalid list head")
				}

				if l.Len() != k+1 {
					t.Fatalf("List length mismatch, exp=%d, act=%d", k+1, l.Len())
				}
			}

			if l.level > 0 {
				leveledUp = true
			}

			if l.Len() != len(tt.items) {
				t.Fatalf("Length mismatch, exp=%d, act=%d", len(tt.items), l.Len())
			}

			sorted := make(sort.IntSlice, len(tt.items))
			copy(sorted, tt.items)
			if tt.asc {
				sorted.Sort()
			} else {
				sort.Sort(sort.Reverse(sorted))
			}

			// Verify the items are in order on level 0 in BOTH directions.
			for i := 0; i < len(tt.items); i++ {
				if x.next[0].item != sorted[i] {
					t.Fatalf("Next item mismatch, i=%d, a=%+v, b=%+v", i, x.next[0].item, sorted[i])
				}

				if i > 0 && x.next[0].prev.item != sorted[i-1] {
					t.Fatalf("Previous item mismatch, i=%d, a=%+v, b=%+v", i, x.next[0].prev.item, sorted[i-1])
				}

				x = x.next[0]
			}

			if x.next[0] != nil {
				t.Fatalf("End of list mismatch, next=%+v", x.next[0])
			}

			for k, v := range tt.items {
				if !l.Delete(v) {
					t.Fatalf("Delete failed")
				}

				if l.Len() != (len(tt.items) - k - 1) {
					t.Fatalf("List len mismatch, exp=%d, act=%d", (len(tt.items) - k - 1), l.Len())
				}

				idx := -1
				for kk, vv := range sorted {
					if vv == v {
						idx = kk
						break
					}
				}

				if idx == -1 {
					t.Fatalf("Unable to find value in sorted items")
				}

				// Delete from sortered list.
				sorted = append(sorted[:idx], sorted[idx+1:]...)

				x := l.head
				if len(sorted) == 0 {
					if x.next[0] != nil {
						t.Fatalf("Expected end of list.next to be nil, next=%+v", x.next[0])
					}
				}

				for i := 0; i < len(sorted); i++ {
					if x.next[0].item != sorted[i] {
						t.Fatalf("Next item mismatch, i=%d, a=%+v, b=%+v", i, x.next[0].item, sorted[i])
					}

					if i > 0 && x.next[0].prev.item != sorted[i-1] {
						t.Fatalf("Previous item mismatch, i=%d, a=%+v, b=%+v", i, x.next[0].prev.item, sorted[i-1])
					}

					x = x.next[0]
				}
			}

			if leveledUp {
				break
			}
		} // End for {}
	} // End range tests
}

func TestExists(t *testing.T) {
	l := New(compareIntAsc)
	a := 1

	if l.Exists(a) {
		t.Fatalf("Unexpected item found")
	}

	if !l.Insert(a) {
		t.Fatalf("Insert failed")
	}

	if !l.Exists(a) {
		t.Fatalf("Expected item to be found")
	}
}

// Test search path given skiplist:
//    4
//   345
// 123456
// Validates to ensure correct & optimal path is taken.
func TestSearchPath(t *testing.T) {
	var visits []int
	compare := func(a interface{}, b interface{}) int {
		visits = append(visits, a.(int))
		return compareIntAsc(a, b)
	}
	l := New(compare)

	nodes := make([]*Node, 6)
	nodes[0] = &Node{item: 1, next: make([]*Node, 1)}
	nodes[1] = &Node{item: 2, next: make([]*Node, 1)}
	nodes[2] = &Node{item: 3, next: make([]*Node, 2)}
	nodes[3] = &Node{item: 4, next: make([]*Node, 3)}
	nodes[4] = &Node{item: 5, next: make([]*Node, 2)}
	nodes[5] = &Node{item: 6, next: make([]*Node, 1)}

	nodes[0].next[0] = nodes[1]
	nodes[1].next[0] = nodes[2]
	nodes[2].next[0] = nodes[3]
	nodes[2].next[1] = nodes[3]
	nodes[3].next[0] = nodes[4]
	nodes[3].next[1] = nodes[4]
	nodes[3].next[2] = nil
	nodes[4].next[0] = nodes[5]
	nodes[4].next[1] = nil

	l.head.next[0] = nodes[0]
	l.head.next[1] = nodes[2]
	l.head.next[2] = nodes[3]
	l.level = 2

	var expVisits []int
	var ok bool

	visits = make([]int, 0)
	expVisits = []int{4, 5, 6}
	ok = l.Exists(6)
	if !ok || len(visits) != len(expVisits) {
		t.Fatalf("Visits mismatch, exp=%+v, act=%+v", expVisits, visits)
	}
	for i := range expVisits {
		if expVisits[i] != visits[i] {
			t.Fatalf("Visits mismatch, exp=%+v, act=%+v", expVisits, visits)
		}
	}

	visits = make([]int, 0)
	expVisits = []int{4, 5}
	ok = l.Exists(5)
	if !ok || len(visits) != len(expVisits) {
		t.Fatalf("Visits mismatch, exp=%+v, act=%+v", expVisits, visits)
	}
	for i := range expVisits {
		if expVisits[i] != visits[i] {
			t.Fatalf("Visits mismatch, exp=%+v, act=%+v", expVisits, visits)
		}
	}

	visits = make([]int, 0)
	expVisits = []int{4}
	ok = l.Exists(4)
	if !ok || len(visits) != len(expVisits) {
		t.Fatalf("Visits mismatch, exp=%+v, act=%+v", expVisits, visits)
	}
	for i := range expVisits {
		if expVisits[i] != visits[i] {
			t.Fatalf("Visits mismatch, exp=%+v, act=%+v", expVisits, visits)
		}
	}

	visits = make([]int, 0)
	expVisits = []int{4, 3}
	ok = l.Exists(3)
	if !ok || len(visits) != len(expVisits) {
		t.Fatalf("Visits mismatch, exp=%+v, act=%+v", expVisits, visits)
	}
	for i := range expVisits {
		if expVisits[i] != visits[i] {
			t.Fatalf("Visits mismatch, exp=%+v, act=%+v", expVisits, visits)
		}
	}

	visits = make([]int, 0)
	expVisits = []int{4, 3}
	ok = l.Exists(3)
	if !ok || len(visits) != len(expVisits) {
		t.Fatalf("Visits mismatch, exp=%+v, act=%+v", expVisits, visits)
	}
	for i := range expVisits {
		if expVisits[i] != visits[i] {
			t.Fatalf("Visits mismatch, exp=%+v, act=%+v", expVisits, visits)
		}
	}

	visits = make([]int, 0)
	expVisits = []int{4, 3, 1, 2}
	ok = l.Exists(2)
	if !ok || len(visits) != len(expVisits) {
		t.Fatalf("Visits mismatch, exp=%+v, act=%+v", expVisits, visits)
	}
	for i := range expVisits {
		if expVisits[i] != visits[i] {
			t.Fatalf("Visits mismatch, exp=%+v, act=%+v", expVisits, visits)
		}
	}

	visits = make([]int, 0)
	expVisits = []int{4, 3, 1}
	ok = l.Exists(1)
	if !ok || len(visits) != len(expVisits) {
		t.Fatalf("Visits mismatch, exp=%+v, act=%+v", expVisits, visits)
	}
	for i := range expVisits {
		if expVisits[i] != visits[i] {
			t.Fatalf("Visits mismatch, exp=%+v, act=%+v", expVisits, visits)
		}
	}
}

func TestIterator(t *testing.T) {
	l := New(compareIntAsc)
	items := []int{1, 2, 3, 4}
	for _, v := range items {
		if ok := l.Insert(v); !ok {
			t.Fatalf("Insert failed")
		}
	}

	order := []int{}
	it := l.Iterator()
	item := it.Current()
	order = append(order, item.(int))
	item = it.Next()
	order = append(order, item.(int))

	expOrder := items[0:2]
	for k, v := range expOrder {
		if order[k] != v {
			t.Fatalf("Iterator mismatch, exp=%d, act=%d", v, order[k])
		}
	}

	ok := it.Seek(1)
	if !ok {
		t.Fatalf("Seek failure")
	}

	item = it.Next()
	if item.(int) != 4 {
		t.Fatalf("Item mismatch, exp=4, act=%d", item.(int))
	}
}

func TestIteratorEmpty(t *testing.T) {
	l := New(compareIntAsc)
	it := l.Iterator()
	if it.Current() != nil || it.Next() != nil || it.Seek(1) {
		t.Fatalf("Expected nil iterator")
	}
}

func compareIntAsc(a interface{}, b interface{}) int {
	aa := a.(int)
	bb := b.(int)

	if aa < bb {
		return -1
	}

	if aa > bb {
		return 1
	}

	return 0
}

func compareIntDesc(a interface{}, b interface{}) int {
	r := compareIntAsc(a, b)
	if r == 1 {
		return -1
	}

	if r == -1 {
		return 1
	}

	return r
}
