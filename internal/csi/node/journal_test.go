package node

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestJournalCrashBoundaries(t *testing.T) {
	for _, at := range []string{"file-synced", "renamed", "directory-synced"} {
		t.Run(at, func(t *testing.T) {
			root := t.TempDir()
			j, e := NewJournal(root)
			if e != nil {
				t.Fatal(e)
			}
			s := LocalState{Key: "clo:volume-a", Initialization: "Prepared"}
			if e = j.Save(s); e != nil {
				t.Fatal(e)
			}
			j.Hook = func(stage string) error {
				if stage == at {
					return errors.New("injected crash")
				}
				return nil
			}
			s.Initialization = "FormatDispatched"
			if e = j.Save(s); e == nil {
				t.Fatal("injection missed")
			}
			reopened, e := NewJournal(root)
			if e != nil {
				t.Fatal(e)
			}
			got, e := reopened.Load(s.Key)
			if e != nil {
				t.Fatal(e)
			}
			want := "FormatDispatched"
			if at == "file-synced" {
				want = "Prepared"
			}
			if got.Initialization != want {
				t.Fatalf("boundary %s got %s", at, got.Initialization)
			}
		})
	}
	root := t.TempDir()
	j, e := NewJournal(root)
	if e != nil {
		t.Fatal(e)
	}
	other, e := NewJournal(root)
	if e != nil {
		t.Fatal(e)
	}
	if e = j.Save(LocalState{Key: "counter"}); e != nil {
		t.Fatal(e)
	}
	var wg sync.WaitGroup
	for _, writer := range []*Journal{j, other} {
		wg.Add(1)
		go func(j *Journal) {
			defer wg.Done()
			for range 10 {
				if e := j.WithLock(context.Background(), "counter", func() error {
					s, e := j.Load("counter")
					if e != nil {
						return e
					}
					s.Generation++
					return j.Save(s)
				}); e != nil {
					t.Error(e)
				}
			}
		}(writer)
	}
	wg.Wait()
	s, e := j.Load("counter")
	if e != nil || s.Generation != 20 {
		t.Fatalf("flock lost update: %+v %v", s, e)
	}
}
