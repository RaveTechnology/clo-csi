package channel

import (
	"context"
	"encoding/json"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"reflect"
	"sync"
)

type queued struct {
	job       protocol.Job
	receipt   protocol.Receipt
	done      chan struct{}
	completed bool
	waiters   int
}
type Queue struct {
	mu      sync.Mutex
	jobs    map[string]*queued
	order   []string
	changed chan struct{}
}

func NewQueue() *Queue   { return &Queue{jobs: map[string]*queued{}, changed: make(chan struct{})} }
func (q *Queue) notify() { close(q.changed); q.changed = make(chan struct{}) }
func copyReceipt(r protocol.Receipt) protocol.Receipt {
	raw, _ := json.Marshal(r)
	var out protocol.Receipt
	_ = json.Unmarshal(raw, &out)
	return out
}
func (q *Queue) Exchange(ctx context.Context, job protocol.Job) (protocol.Receipt, error) {
	q.mu.Lock()
	entry := q.jobs[job.ID]
	if entry != nil && entry.job != job {
		q.mu.Unlock()
		return protocol.Receipt{}, protocol.Fail(protocol.Blocked, "job ID reused with different identity")
	}
	if entry == nil {
		if len(q.order) >= 4096 {
			for i, id := range q.order {
				if old := q.jobs[id]; old == nil || old.completed && old.waiters == 0 {
					delete(q.jobs, id)
					q.order = append(q.order[:i], q.order[i+1:]...)
					break
				}
			}
		}
		if len(q.order) >= 4096 {
			q.mu.Unlock()
			return protocol.Receipt{}, protocol.Fail(protocol.Unavailable, "node job queue full")
		}
		entry = &queued{job: job, done: make(chan struct{})}
		q.jobs[job.ID] = entry
		q.order = append(q.order, job.ID)
		q.notify()
	}
	entry.waiters++
	q.mu.Unlock()
	defer func() {
		q.mu.Lock()
		defer q.mu.Unlock()
		entry.waiters--
		if !entry.completed && entry.waiters == 0 {
			delete(q.jobs, job.ID)
			for i, id := range q.order {
				if id == job.ID {
					q.order = append(q.order[:i], q.order[i+1:]...)
					break
				}
			}
			q.notify()
		}
	}()
	select {
	case <-ctx.Done():
		return protocol.Receipt{}, ctx.Err()
	case <-entry.done:
		q.mu.Lock()
		r := copyReceipt(entry.receipt)
		q.mu.Unlock()
		return r, nil
	}
}
func (q *Queue) Poll(ctx context.Context, node protocol.NodeRef) (protocol.Job, error) {
	for {
		if e := ctx.Err(); e != nil {
			return protocol.Job{}, e
		}
		q.mu.Lock()
		for _, id := range q.order {
			entry := q.jobs[id]
			if entry != nil && !entry.completed && entry.job.Node == node {
				job := entry.job
				q.mu.Unlock()
				return job, nil
			}
		}
		changed := q.changed
		q.mu.Unlock()
		select {
		case <-ctx.Done():
			return protocol.Job{}, ctx.Err()
		case <-changed:
		}
	}
}
func (q *Queue) Complete(ctx context.Context, node protocol.NodeRef, r protocol.Receipt) error {
	if e := ctx.Err(); e != nil {
		return e
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	entry := q.jobs[r.JobID]
	if entry == nil {
		return protocol.Fail(protocol.NotFound, "node job no longer pending")
	}
	job := entry.job
	if node != job.Node || r.Node != job.Node || r.Volume != job.Volume || r.Nonce != job.Nonce || r.Type != job.Type {
		return protocol.Fail(protocol.Blocked, "node receipt owner/epoch/nonce differs")
	}
	r = copyReceipt(r)
	if entry.completed {
		if !reflect.DeepEqual(entry.receipt, r) {
			return protocol.Fail(protocol.Blocked, "completed job receipt differs")
		}
		return nil
	}
	entry.receipt = r
	entry.completed = true
	close(entry.done)
	q.notify()
	return nil
}
