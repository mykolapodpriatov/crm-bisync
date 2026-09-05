package engine

import (
	"sort"
	"sync"
	"time"

	"crm-bisync/internal/model"
)

// task is one unit of work: a change to consider, and how many times it has
// already been considered.
type task struct {
	Sync     string
	Event    model.ChangeEvent
	Attempts int
	// NotBefore holds a retry back until its backoff has elapsed.
	NotBefore time.Time
	// done is closed when the task reaches a terminal state. Pollers wait on
	// it before advancing a watermark, because a mark that moves past work
	// still in flight is a mark that skips records if the process restarts.
	done chan struct{}
}

// finish marks a task terminal. Safe to call more than once, because a task
// can be finished by its worker and abandoned by shutdown.
func (t *task) finish() {
	if t.done == nil {
		return
	}
	select {
	case <-t.done:
	default:
		close(t.done)
	}
}

// workQueue is the in-process queue feeding the worker pool.
//
// It is deliberately not durable. Durability lives in the watermark, which
// does not advance past unfinished work, and in the dead-letter queue, which
// does. A crash therefore loses only the in-flight retries, and the next poll
// re-reads them, which is the behaviour the overlap window and idempotent
// writes were built for.
type workQueue struct {
	mu      sync.Mutex
	ready   []*task
	delayed []*task
	closed  bool
	wake    chan struct{}
	// depth is the number of tasks queued or in flight, which is what the
	// metrics report and what shutdown waits on.
	depth int
	empty chan struct{}
}

func newWorkQueue(workers int) *workQueue {
	if workers < 1 {
		workers = 1
	}
	return &workQueue{
		wake:  make(chan struct{}, workers),
		empty: make(chan struct{}, 1),
	}
}

// push adds a task that is ready now.
func (q *workQueue) push(t *task) bool {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		t.finish()
		return false
	}
	q.ready = append(q.ready, t)
	q.depth++
	q.mu.Unlock()

	q.signal()
	return true
}

// pushDelayed adds a task that becomes ready at t.NotBefore.
func (q *workQueue) pushDelayed(t *task) bool {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		t.finish()
		return false
	}
	q.delayed = append(q.delayed, t)
	// Sorted so the soonest retry is always first, which keeps promote from
	// scanning the whole set on every poll of the queue.
	sort.SliceStable(q.delayed, func(i, j int) bool {
		return q.delayed[i].NotBefore.Before(q.delayed[j].NotBefore)
	})
	q.depth++
	q.mu.Unlock()

	q.signal()
	return true
}

func (q *workQueue) signal() {
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

// promote moves delayed tasks whose backoff has elapsed onto the ready list.
func (q *workQueue) promote(now time.Time) {
	q.mu.Lock()
	defer q.mu.Unlock()

	i := 0
	for ; i < len(q.delayed); i++ {
		if q.delayed[i].NotBefore.After(now) {
			break
		}
		q.ready = append(q.ready, q.delayed[i])
	}
	q.delayed = q.delayed[i:]
}

// next takes a ready task, or reports false when there is none.
func (q *workQueue) next(now time.Time) (*task, bool) {
	q.promote(now)

	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.ready) == 0 {
		return nil, false
	}
	t := q.ready[0]
	q.ready = q.ready[1:]
	return t, true
}

// complete records that a task reached a terminal state.
func (q *workQueue) complete(t *task) {
	t.finish()

	q.mu.Lock()
	q.depth--
	drained := q.depth == 0
	q.mu.Unlock()

	if drained {
		select {
		case q.empty <- struct{}{}:
		default:
		}
	}
}

// requeue puts a task back without changing the depth, because it never left.
func (q *workQueue) requeue(t *task) {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		t.finish()
		return
	}
	q.delayed = append(q.delayed, t)
	sort.SliceStable(q.delayed, func(i, j int) bool {
		return q.delayed[i].NotBefore.Before(q.delayed[j].NotBefore)
	})
	q.mu.Unlock()

	q.signal()
}

// Depth reports how many tasks are queued or in flight.
func (q *workQueue) Depth() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.depth
}

// nextDeadline reports when the soonest delayed task becomes ready.
func (q *workQueue) nextDeadline() (time.Time, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.delayed) == 0 {
		return time.Time{}, false
	}
	return q.delayed[0].NotBefore, true
}

// close stops the queue accepting work and abandons what is left, so that
// anything waiting on a task's completion is released rather than hanging.
func (q *workQueue) close() {
	q.mu.Lock()
	q.closed = true
	pending := append(q.ready, q.delayed...)
	q.ready, q.delayed = nil, nil
	q.depth = 0
	q.mu.Unlock()

	for _, t := range pending {
		t.finish()
	}
	select {
	case q.empty <- struct{}{}:
	default:
	}
}

// keyedMutex serialises work on one logical record.
//
// Without it the engine has a real duplicate-create race. A change on the left
// creates a record on the right; the create's echo arrives from the right
// before the left has finished writing the link and indexing the identity
// keys; identity resolution finds neither, decides to create, and the record
// exists twice. Locking on the identity fingerprint rather than on either
// remote ID is what makes the two events collide, since at that point they are
// the only thing the two sides have in common.
type keyedMutex struct {
	mu    sync.Mutex
	held  map[string]*sync.Mutex
	count map[string]int
}

func newKeyedMutex() *keyedMutex {
	return &keyedMutex{held: map[string]*sync.Mutex{}, count: map[string]int{}}
}

// lock takes the lock for a key and returns the function that releases it.
func (k *keyedMutex) lock(key string) func() {
	k.mu.Lock()
	m, ok := k.held[key]
	if !ok {
		m = &sync.Mutex{}
		k.held[key] = m
	}
	k.count[key]++
	k.mu.Unlock()

	m.Lock()

	return func() {
		m.Unlock()

		k.mu.Lock()
		k.count[key]--
		if k.count[key] == 0 {
			// Dropped once nobody holds or waits for it, so a long-running
			// process does not accumulate one mutex per record it has ever
			// seen.
			delete(k.held, key)
			delete(k.count, key)
		}
		k.mu.Unlock()
	}
}
