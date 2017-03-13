package skiplist

import (
	"errors"
	"math/rand"
	"sync"
	"time"
)

// Skiplist implementation based on
// Skip Lists: A Probabilistic Alternative to Balanced Trees paper.
// Minimal version created to be utilized as a searchable priority queue.
// Allows for custom comparison function and any value to be stored in the list
// Level 0 holds both backward and forward pointers.
const (
	maxLevel       = 32 // Accounts for distribution on 2^32 items
	probability    = 4  // 1/4
	compareExact   = 0  // Compare exact match return value
	compareGreater = 1
)

var (
	ErrDuplicate = errors.New("Duplicate")
)

// Compare func interface
// Allows for users of Skiplist to define sort strategy for skiplists
// Value "a" is the base subject of the comparison or the "left" side.
// Value "b" is the new subject or the "right" side.
// There are 3 possible int return values:
// "0" - The 2 values are exact matches.
// "1" - Value "a" is greather than value "b"
// "-1" - Value "a" is less than value "b"
type Compare func(a interface{}, b interface{}) int

type List struct {
	level   int     // Highest level in list
	len     int     // Len of all items
	head    *Node   // Head of list
	compare Compare // Comparison func
	rand    *rand.Rand
	mu      sync.RWMutex
}

type Node struct {
	item interface{} // Item value as interface
	prev *Node       // Previous node, only for level 0
	next []*Node     // Slice of next nodes, up to maxLevel
}

// New Skiplist
// Requires a compare func honoring the interface
func New(compare Compare) *List {
	head := &Node{next: make([]*Node, maxLevel)}
	return &List{
		level:   0,
		head:    head,
		compare: compare,
		rand:    rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// "Fix the dice" strategy
// Generate a level that is at most 1 higher than current max
func (l *List) randLevel() int {
	var level int
	for level = 0; l.rand.Int31n(probability) == 1 && level < maxLevel; level++ {
		if level > l.level {
			break
		}
	}

	return level
}

// Insert a new item
// Returns false if the item already exist
// Returns true on successful insert
func (l *List) Insert(item interface{}) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	level := l.randLevel()
	if level > l.level {
		l.level = level
	}

	inserted := false
	node := &Node{item: item, next: make([]*Node, level+1)}
	x := l.head
	for i := l.level; i >= 0; i-- {
		for x.next[i] != nil {
			// Found larger next item, slot found.
			compare := l.compare(x.next[i].item, item)

			// Fail on duplicate.
			if compare == compareExact {
				return false
			}

			// If next item is greater than insert subject.
			// Found correct slot.
			if compare == 1 {
				break
			}

			// No match found, move forward.
			x = x.next[i]
		}

		// Keep moving down
		if i > level {
			continue
		}

		node.next[i] = x.next[i]
		// On level 0, link up previous nodes.
		if i == 0 {
			node.prev = x
			if x.next[i] != nil {
				x.next[i].prev = node
			}
		}
		x.next[i] = node

		if !inserted {
			inserted = true
		}

		// No match found on this level, move down on next iteration.
	}

	if inserted {
		l.len++
	}
	return inserted
}

// Verifies if an item is in the queue
func (l *List) Exists(item interface{}) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	x := l.head
	for i := l.level; i >= 0; i-- {
		for x.next[i] != nil {
			compare := l.compare(x.next[i].item, item)
			if compare == compareExact {
				return true
			}
			// Found larger next item, slot found
			if compare == compareGreater {
				break
			}

			// No match found, move forward.
			x = x.next[i]
		}

		// No match found on this level, move down on next iteration.
	}

	return false
}

// Pop an item from the head
// Follows the comma ok idiom.
// bool is false if no items are available
func (l *List) Pop() (interface{}, bool) {
	l.mu.RLock()
	node := l.head.next[0]
	l.mu.RUnlock()
	if node == nil {
		return nil, false
	}

	l.Delete(node.item)
	return node.item, true
}

// Delete an item by value
// Uses the initilized Compare func
func (l *List) Delete(item interface{}) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	var found = false
	var prev, x *Node
	x = l.head
	for i := l.level; i >= 0; i-- {
		for x.next[i] != nil {
			compare := l.compare(x.next[i].item, item)
			// Found exact match.
			if compare == 0 {
				found = true
				break
			}

			// Item not found on this level.
			if compare == compareGreater {
				break
			}

			x = x.next[i]
		}

		if !found {
			continue
		}

		prev = x
		next := x.next[i].next[i]
		prev.next[i] = next

		// O level has both directions linked.
		if i == 0 {
			if next != nil {
				next.prev = prev
			}
		}

		// Determine if we have deleted the max item in current level.
		// If so, reduce max level of list.
		if next == nil && prev == l.head && i > 0 {
			l.level = i - 1
		}
	}

	if found {
		l.len--
	}

	return found
}

// Lengh of items in list
func (l *List) Len() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.len
}

func (l *List) Iterator() *Iterator {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return &Iterator{x: l.head.next[0], mu: &l.mu}
}

// Concurrent-safe iterator to scan the list qithout removing items.
type Iterator struct {
	x  *Node
	mu *sync.RWMutex
}

// Return the next item
func (it *Iterator) Next() interface{} {
	it.mu.RLock()
	defer it.mu.RUnlock()
	if it.x != nil && it.x.next[0] != nil {
		node := it.x.next[0]
		it.x = node
		return node.item
	}

	return nil
}

// Return the current item
func (it *Iterator) Current() interface{} {
	it.mu.RLock()
	defer it.mu.RUnlock()
	if it.x != nil {
		return it.x.item
	}

	return nil
}

// Seek to the n-th offset
// Returns a bool to signal whether the full seek was successful.
// Will seek to the nearest n-th if n is out of range.
func (it *Iterator) Seek(n int) bool {
	it.mu.RLock()
	defer it.mu.RUnlock()

	for i := 0; i < n; i++ {
		if it.x == nil || it.x.next[0] == nil {
			return false
		}

		node := it.x.next[0]
		it.x = node
	}

	return true
}
