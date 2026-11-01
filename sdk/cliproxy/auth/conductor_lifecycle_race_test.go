package auth

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"testing/synctest"
)

// Run with -race to check that scheduler snapshots never read published auths unlocked.
func TestPublishedAuthSnapshotRace(t *testing.T) {
	for _, operation := range []string{"Register", "Update", "UpdatePreparedAuth", "UpdateRefreshedAuth"} {
		t.Run(operation, func(t *testing.T) {
			ctx := WithSkipPersist(context.Background())
			m := NewManager(nil, nil, nil)
			a := &Auth{
				ID: "snapshot-race", Provider: "audit", Status: StatusActive,
				Metadata: map[string]any{"type": "audit"},
			}
			for i := 0; i < 200; i++ {
				a.Metadata[fmt.Sprintf("field-%d", i)] = i
			}
			if _, errRegister := m.Register(ctx, a); errRegister != nil {
				t.Fatal(errRegister)
			}

			start := make(chan struct{})
			var wg sync.WaitGroup
			for worker := 0; worker < 32; worker++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					for i := 0; i < 100; i++ {
						m.MarkResult(ctx, Result{
							AuthID: a.ID, Provider: a.Provider,
							Model: fmt.Sprintf("model-%d", i%8), Success: true,
						})
					}
				}()
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				for i := 0; i < 100; i++ {
					base, ok := m.GetByID(a.ID)
					if !ok {
						t.Error("missing auth")
						return
					}
					updated := base.Clone()
					updated.Metadata["note"] = fmt.Sprint(i)
					var errPublish error
					switch operation {
					case "Register":
						_, errPublish = m.Register(ctx, updated)
					case "Update":
						_, errPublish = m.Update(ctx, updated)
					case "UpdatePreparedAuth":
						_, errPublish = m.UpdatePreparedAuth(ctx, base, updated)
					case "UpdateRefreshedAuth":
						_, errPublish = m.UpdateRefreshedAuth(ctx, base, updated)
					}
					if errPublish != nil {
						t.Error(errPublish)
						return
					}
				}
			}()
			close(start)
			wg.Wait()
		})
	}
}

func TestManagerModifyWaitsForConditionalRemoval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := WithSkipPersist(context.Background())
		manager := NewManager(nil, nil, nil)
		auth, errRegister := manager.Register(ctx, &Auth{ID: "modify-removal", Provider: "audit", Status: StatusActive})
		if errRegister != nil {
			t.Fatal(errRegister)
		}
		deletionEntered := make(chan struct{})
		allowDeletion := make(chan struct{})
		removed := make(chan error, 1)
		go func() {
			didRemove, errRemove := manager.RemoveIf(ctx, auth.ID, nil, func(*Auth) error {
				close(deletionEntered)
				<-allowDeletion
				return nil
			})
			if errRemove == nil && !didRemove {
				errRemove = fmt.Errorf("credential was not removed")
			}
			removed <- errRemove
		}()
		<-deletionEntered
		modified := make(chan error, 1)
		editCalled := false
		go func() {
			updated, didModify, errModify := manager.Modify(ctx, auth.ID, auth.RegistrationEpoch, func(current *Auth) error {
				editCalled = true
				current.Label = "modified"
				return nil
			})
			if errModify == nil && (updated != nil || didModify) {
				errModify = fmt.Errorf("modified credential during removal")
			}
			modified <- errModify
		}()
		synctest.Wait()
		select {
		case <-modified:
			t.Error("Modify completed before the pending removal")
		default:
		}
		if editCalled {
			t.Error("Modify edited credential during removal")
		}
		close(allowDeletion)
		if errRemove := <-removed; errRemove != nil {
			t.Fatal(errRemove)
		}
		if errModify := <-modified; errModify != nil {
			t.Fatal(errModify)
		}
		if _, ok := manager.GetByID(auth.ID); ok {
			t.Fatal("Modify resurrected a removed credential")
		}
	})
}

func TestManagerStaleUnauthorizedResultReleasesLifecycle(t *testing.T) {
	SetDeleteUnauthorizedAuth(true)
	t.Cleanup(func() { SetDeleteUnauthorizedAuth(false) })

	synctest.Test(t, func(t *testing.T) {
		ctx := WithSkipPersist(context.Background())
		manager := NewManager(nil, nil, nil)
		initial, errRegister := manager.Register(ctx, &Auth{ID: "stale-401", Provider: "codex"})
		if errRegister != nil {
			t.Fatal(errRegister)
		}
		replacement, errReplace := manager.Register(ctx, initial.Clone())
		if errReplace != nil {
			t.Fatal(errReplace)
		}
		manager.MarkResult(ctx, Result{
			AuthID: initial.ID, Provider: initial.Provider, RegistrationEpoch: initial.RegistrationEpoch,
			Error: &Error{HTTPStatus: http.StatusUnauthorized, Message: "stale unauthorized"},
		})
		if _, errUpdate := manager.Update(ctx, replacement); errUpdate != nil {
			t.Fatalf("replacement update after stale 401: %v", errUpdate)
		}
		if current, ok := manager.GetByID(initial.ID); !ok || current.RegistrationEpoch != replacement.RegistrationEpoch {
			t.Fatal("stale 401 removed the replacement credential")
		}
	})
}

func TestManagerRemoveIfMissingAuth(t *testing.T) {
	for _, tc := range []struct {
		name     string
		matches  func(*Auth) bool
		wantCall bool
	}{
		{name: "default rejects missing"},
		{name: "predicate rejects missing", matches: func(current *Auth) bool { return current != nil }},
		{name: "predicate accepts missing", matches: func(current *Auth) bool { return current == nil }, wantCall: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manager := NewManager(nil, nil, nil)
			called := false
			removed, errRemove := manager.RemoveIf(context.Background(), "missing", tc.matches, func(current *Auth) error {
				called = true
				if current != nil {
					t.Fatal("missing credential callback received a non-nil snapshot")
				}
				return nil
			})
			if errRemove != nil || removed || called != tc.wantCall {
				t.Fatalf("RemoveIf = (%v, %v), callback=%v; want false, nil, callback=%v", removed, errRemove, called, tc.wantCall)
			}
		})
	}
}

func TestManagerRemoveIfExternalFailurePreservesRuntime(t *testing.T) {
	ctx := WithSkipPersist(context.Background())
	manager := NewManager(nil, nil, nil)
	auth, errRegister := manager.Register(ctx, &Auth{ID: "delete-fails", Provider: "audit"})
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	deleteErr := errors.New("external deletion failed")
	removed, errRemove := manager.RemoveIf(ctx, auth.ID, nil, func(*Auth) error { return deleteErr })
	if removed || !errors.Is(errRemove, deleteErr) {
		t.Fatalf("RemoveIf = (%v, %v), want false and external error", removed, errRemove)
	}
	if current, ok := manager.GetByID(auth.ID); !ok || current.RegistrationEpoch != auth.RegistrationEpoch {
		t.Fatal("failed external deletion changed the runtime credential")
	}
}

func TestManagerConditionalRollbackBlocksRegistration(t *testing.T) {
	for _, exists := range []bool{false, true} {
		t.Run(fmt.Sprintf("existing=%v", exists), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx := WithSkipPersist(context.Background())
				manager := NewManager(nil, nil, nil)
				if exists {
					if _, errRegister := manager.Register(ctx, &Auth{ID: "pending", Provider: "audit"}); errRegister != nil {
						t.Fatal(errRegister)
					}
				}
				entered := make(chan struct{})
				allowRollback := make(chan struct{})
				rolledBack := make(chan error, 1)
				go func() {
					removed, errRemove := manager.RemoveIf(ctx, "pending", func(current *Auth) bool { return (current != nil) == exists }, func(*Auth) error {
						close(entered)
						<-allowRollback
						return nil
					})
					if removed != exists && errRemove == nil {
						errRemove = fmt.Errorf("removed=%v, want %v", removed, exists)
					}
					rolledBack <- errRemove
				}()
				<-entered
				registered := make(chan error, 1)
				go func() {
					_, errRegister := manager.Register(ctx, &Auth{ID: "pending", Provider: "audit", Label: "replacement"})
					registered <- errRegister
				}()
				synctest.Wait()
				select {
				case <-registered:
					t.Fatal("registration completed before external rollback")
				default:
				}
				close(allowRollback)
				if errRollback := <-rolledBack; errRollback != nil {
					t.Fatal(errRollback)
				}
				if errRegister := <-registered; errRegister != nil {
					t.Fatal(errRegister)
				}
				if current, ok := manager.GetByID("pending"); !ok || current.Label != "replacement" {
					t.Fatal("rollback removed the later registration")
				}
			})
		})
	}
}
