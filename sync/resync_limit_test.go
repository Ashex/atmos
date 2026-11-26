package sync_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/internal/testutil"
	"github.com/jcalabro/atmos/repo"
	"github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
)

func TestResyncLimitSharedThroughApply(t *testing.T) {
	t.Parallel()
	const inline, background atmos.DID = "did:plc:inline", "did:plc:background"
	keyA, carA, _, _, _ := buildChainBreak(t, inline)
	keyB, carB, store, _, trigger := buildChainBreak(t, background)
	_, commit, err := repo.LoadFromCAR(bytes.NewReader(carA))
	require.NoError(t, err)
	fetchedB := make(chan struct{}, 2)
	xc := testutil.NewFakeSyncServerMulti(t, func(did atmos.DID) ([]byte, bool) {
		switch did {
		case inline:
			return carA, true
		case background:
			fetchedB <- struct{}{}
			return carB, true
		default:
			return nil, false
		}
	})
	resolver := testutil.NewTrackingResolver()
	resolver.Docs[inline] = testutil.BuildDIDDoc(inline, keyA.PublicKey())
	resolver.Docs[background] = testutil.BuildDIDDoc(background, keyB.PublicKey())
	dir := &identity.Directory{Resolver: resolver}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	applying, resume := make(chan struct{}), make(chan struct{})
	v, err := sync.NewVerifier(sync.VerifierOptions{
		Directory: dir, StateStore: store, MaxConcurrentResyncs: 1,
		SyncClient: gt.Some(sync.NewClient(sync.Options{Client: xc, Directory: gt.Some(dir)})),
		OnResync: gt.Some(func(did atmos.DID, _, _ string, _ sync.ResyncReason) {
			if did == inline {
				close(applying)
				select {
				case <-resume:
				case <-ctx.Done():
				}
			}
		}),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, v.Close()) })
	done := make(chan error, 1)
	go func() {
		_, err := v.VerifySync(ctx, &comatproto.SyncSubscribeRepos_Sync{DID: string(inline), Rev: commit.Rev})
		done <- err
	}()
	select {
	case <-applying:
	case <-ctx.Done():
		t.Fatal("inline resync never reached apply")
	}
	ops, err := v.VerifyCommit(ctx, trigger())
	require.NoError(t, err)
	require.Nil(t, ops)
	wait, stop := context.WithTimeout(ctx, 100*time.Millisecond)
	defer stop()
	_, err = v.Resync(wait, background)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	select {
	case <-fetchedB:
		t.Fatal("second repo fetched while inline apply held the slot")
	default:
	}
	close(resume)
	require.NoError(t, <-done)
	select {
	case event := <-v.ResyncEvents():
		require.Equal(t, background, event.DID)
		require.NotEmpty(t, event.Ops)
	case err := <-v.AsyncErrors():
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal("queued background resync failed to complete")
	}
}
