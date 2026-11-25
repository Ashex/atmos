package sync_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/car"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/internal/testutil"
	"github.com/jcalabro/atmos/mst"
	"github.com/jcalabro/atmos/repo"
	"github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
)

func TestResyncDiskStore_SignedPayloads(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	did := atmos.DID("did:plc:test123")
	initialCAR, key := buildTestRepo(t, 256)
	original, _, err := repo.LoadFromCAR(bytes.NewReader(initialCAR))
	require.NoError(t, err)
	record := map[string]any{
		"$type": "app.bsky.feed.post", "text": "A repeated post",
		"createdAt": "2024-01-01T00:00:00Z",
	}
	for _, key := range []string{"alias-a", "alias-b"} {
		require.NoError(t, original.Create("app.bsky.feed.post", key, record))
	}
	var archive bytes.Buffer
	require.NoError(t, original.ExportCAR(&archive, key))
	loaded, commit, err := repo.LoadFromCAR(bytes.NewReader(archive.Bytes()))
	require.NoError(t, err)
	header, blocks, err := car.ReadAll(bytes.NewReader(archive.Bytes()))
	require.NoError(t, err)
	var syncBlocks bytes.Buffer
	require.NoError(t, car.WriteAll(&syncBlocks, header.Roots, blocks[:1]))
	_, xc := serveCAR(t, archive.Bytes())
	resolver := testutil.NewTrackingResolver()
	resolver.Docs[did] = testutil.BuildDIDDoc(did, key.PublicKey())
	dir := &identity.Directory{Resolver: resolver}

	var want []sync.VerifierOp
	err = loaded.Tree.Walk(func(path string, cid cbor.CID) error {
		data, err := loaded.Store.GetBlock(cid)
		if err != nil {
			return err
		}
		collection, rkey := repo.SplitMSTKey(path)
		want = append(want, sync.VerifierOp{
			Action: atmos.ActionResync, Repo: did, Rev: atmos.TID(commit.Rev),
			Collection: atmos.NSID(collection), RKey: atmos.RecordKey(rkey),
			CID: cid, BlockData: data,
		})
		return nil
	})
	require.NoError(t, err)
	require.Len(t, want, 258)

	for _, tc := range []struct {
		name  string
		disk  gt.Option[bool]
		quota gt.Option[uint64]
	}{
		{name: "default memory"},
		{name: "explicit disk", disk: gt.Some(true)},
		{name: "quota implies disk", quota: gt.Some(carPayloadBytes(t, archive.Bytes()))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assertSharedPayload := func(ops []sync.VerifierOp) {
				t.Helper()
				require.Equal(t, want, ops)
				require.Equal(t, atmos.RecordKey("alias-a"), ops[0].RKey)
				require.Equal(t, atmos.RecordKey("alias-b"), ops[1].RKey)
				require.Equal(t, ops[0].CID, ops[1].CID)
				require.True(t, &ops[0].BlockData[0] == &ops[1].BlockData[0], "aliases should share payload bytes")
			}

			newVerifier := func() (*sync.Verifier, sync.StateStore) {
				cs := sync.NewMemStateStore()
				v, err := sync.NewVerifier(sync.VerifierOptions{
					Directory: dir, StateStore: cs, ResyncDiskStore: tc.disk, ResyncScratchBytes: tc.quota,
					SyncClient: gt.Some(sync.NewClient(sync.Options{Client: xc, Directory: gt.Some(dir)})),
				})
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, v.Close()) })
				return v, cs
			}

			v, cs := newVerifier()
			ops, err := v.VerifySync(ctx, &comatproto.SyncSubscribeRepos_Sync{
				DID: string(did), Rev: commit.Rev, Blocks: syncBlocks.Bytes(),
			})
			require.NoError(t, err)
			assertSharedPayload(ops)
			state, err := cs.LoadChain(ctx, did)
			require.NoError(t, err)
			require.Equal(t, &sync.ChainState{Rev: commit.Rev, Data: commit.Data}, state)

			async, asyncState := newVerifier()
			mismatched := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("unrelated root"))
			require.NoError(t, asyncState.SaveChain(ctx, did, sync.ChainState{Rev: "3aaaaaaaaaaaa", Data: mismatched}))
			asyncRepo, _, err := repo.LoadFromCAR(bytes.NewReader(archive.Bytes()))
			require.NoError(t, err)
			trigger := testutil.BuildSyntheticCommit(t, asyncRepo, key, commit.Data, []testutil.OpAction{{
				Action: testutil.ActionUpdate, Collection: "app.bsky.feed.post", RKey: "rec0",
				Record: map[string]any{"text": "new post text"},
			}})
			ops, err = async.VerifyCommit(ctx, trigger)
			require.NoError(t, err)
			require.Nil(t, ops)
			select {
			case result := <-async.ResyncEvents():
				require.Equal(t, sync.ReasonChainBreak, result.Reason)
				require.Equal(t, commit.Rev, result.NewRev)
				assertSharedPayload(result.Ops)
			case err := <-async.AsyncErrors():
				t.Fatalf("expected resync event: %v", err)
			case <-time.After(5 * time.Second):
				t.Fatal("resync event timed out")
			}
		})
	}
}

func TestResyncDiskStore_Unavailable(t *testing.T) {
	did := atmos.DID("did:plc:diskunavailable")
	missing := filepath.Join(t.TempDir(), "missing")
	t.Setenv("TMPDIR", missing)
	t.Setenv("TMP", missing)
	t.Setenv("TEMP", missing)
	if os.TempDir() != missing {
		t.Skip("cannot redirect temporary files on this platform")
	}

	for _, tc := range []struct {
		name   string
		disk   gt.Option[bool]
		quota  gt.Option[uint64]
		memory bool
	}{
		{name: "default", memory: true},
		{name: "explicit memory", disk: gt.Some(false), memory: true},
		{name: "zero quota", quota: gt.Some(uint64(0)), memory: true},
		{name: "explicit disk", disk: gt.Some(true)},
		{name: "quota implies disk", quota: gt.Some(uint64(1 << 20))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, archive, cs, dir, trigger := buildChainBreak(t, did)
			_, xc := serveCAR(t, archive)
			v, err := sync.NewVerifier(sync.VerifierOptions{
				Directory: dir, StateStore: cs, ResyncDiskStore: tc.disk, ResyncScratchBytes: tc.quota,
				SyncClient: gt.Some(sync.NewClient(sync.Options{Client: xc, Directory: gt.Some(dir)})),
			})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, v.Close()) })

			ctx := context.Background()
			want, err := cs.LoadChain(ctx, did)
			require.NoError(t, err)
			ops, err := v.Resync(ctx, did)
			if tc.memory {
				require.NoError(t, err)
				require.Len(t, ops, 1)
				return
			}
			require.Nil(t, ops)
			_, classified := errors.AsType[*sync.ResyncFailedError](err)
			require.True(t, classified, "actual error: %v", err)
			require.ErrorIs(t, err, os.ErrNotExist)
			ops, err = v.VerifyCommit(ctx, trigger())
			require.NoError(t, err)
			require.Nil(t, ops)
			select {
			case err := <-v.AsyncErrors():
				require.ErrorIs(t, err, os.ErrNotExist)
				_, classified := errors.AsType[*sync.ResyncFailedError](err)
				require.True(t, classified, "actual error: %v", err)
			case result := <-v.ResyncEvents():
				t.Fatalf("unexpected resync event: %v", result.Reason)
			case <-time.After(5 * time.Second):
				t.Fatal("resync error timed out")
			}
			after, err := cs.LoadChain(ctx, did)
			require.NoError(t, err)
			require.Equal(t, want, after)
		})
	}
}

func TestNewVerifier_ResyncScratchQuotaConflictsWithExplicitMemory(t *testing.T) {
	t.Parallel()

	v, err := sync.NewVerifier(sync.VerifierOptions{
		Directory: &identity.Directory{}, StateStore: sync.NewMemStateStore(),
		Policy: gt.Some(sync.PolicyError), ResyncDiskStore: gt.Some(false),
		ResyncScratchBytes: gt.Some(uint64(1)),
	})
	require.Nil(t, v)
	require.ErrorContains(t, err, "positive ResyncScratchBytes requires ResyncDiskStore")
}

func TestResyncDiskStore_ValidationOrder(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:diskvalidation")
	key, archive, _, _, _ := buildChainBreak(t, did)
	wrongKey, err := crypto.GenerateP256()
	require.NoError(t, err)
	loaded, _, err := repo.LoadFromCAR(bytes.NewReader(archive))
	require.NoError(t, err)
	recordCID, err := loaded.Tree.Get("app.bsky.feed.post/x")
	require.NoError(t, err)
	require.NotNil(t, recordCID)
	partial := stripBlockFromCAR(t, archive, *recordCID)

	header, blocks, err := car.ReadAll(bytes.NewReader(archive))
	require.NoError(t, err)
	// A malformed trailing copy must fail hash validation even though
	// the reusable buffer saw the same CID in a valid block earlier.
	var corrupt bytes.Buffer
	bad := car.Block{CID: *recordCID, Data: []byte("corrupt block")}
	require.NoError(t, car.WriteAll(&corrupt, header.Roots, append(blocks, bad)))

	for _, tc := range []struct {
		name    string
		archive []byte
		pub     crypto.PublicKey
		check   func(*testing.T, error)
	}{
		{"hash before signature", corrupt.Bytes(), wrongKey.PublicKey(), func(t *testing.T, err error) {
			t.Helper()
			require.ErrorContains(t, err, "block CID mismatch")
		}},
		{"signature before walk", partial, wrongKey.PublicKey(), func(t *testing.T, err error) {
			t.Helper()
			_, signatureError := errors.AsType[*sync.SignatureError](err)
			require.True(t, signatureError, "actual error: %v", err)
		}},
		{"valid signature and missing record", partial, key.PublicKey(), func(t *testing.T, err error) {
			t.Helper()
			require.ErrorIs(t, err, mst.ErrBlockNotFound)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, xc := serveCAR(t, tc.archive)
			resolver := testutil.NewTrackingResolver()
			resolver.Docs[did] = testutil.BuildDIDDoc(did, tc.pub)
			dir := &identity.Directory{Resolver: resolver}
			cs := sync.NewMemStateStore()
			v, err := sync.NewVerifier(sync.VerifierOptions{
				Directory: dir, StateStore: cs, ResyncDiskStore: gt.Some(true),
				SyncClient: gt.Some(sync.NewClient(sync.Options{Client: xc, Directory: gt.Some(dir)})),
			})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, v.Close()) })

			ctx := context.Background()
			ops, err := v.Resync(ctx, did)
			require.Nil(t, ops)
			_, classified := errors.AsType[*sync.ResyncFailedError](err)
			require.True(t, classified, "actual error: %v", err)
			tc.check(t, err)
			state, err := cs.LoadChain(ctx, did)
			require.NoError(t, err)
			require.Nil(t, state)
			require.Zero(t, v.Stats().Resyncs)
		})
	}
}

type gatedResyncResolver struct {
	*testutil.TrackingResolver
	hold    atmos.DID
	entered chan<- struct{}
	release <-chan struct{}
}

func (r gatedResyncResolver) ResolveDID(ctx context.Context, did atmos.DID) (*identity.DIDDocument, error) {
	if did == r.hold {
		select {
		case r.entered <- struct{}{}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		select {
		case <-r.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return r.TrackingResolver.ResolveDID(ctx, did)
}

func carPayloadBytes(t *testing.T, archive []byte) uint64 {
	t.Helper()
	_, blocks, err := car.ReadAll(bytes.NewReader(archive))
	require.NoError(t, err)
	var total uint64
	for _, block := range blocks {
		total += uint64(len(block.Data))
	}
	return total
}

func TestResyncDiskStore_AggregateQuota(t *testing.T) {
	t.Parallel()

	for _, outcome := range []string{"applied", "cancelled"} {
		t.Run(outcome, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			firstDID := atmos.DID("did:plc:quotafirst")
			secondDID := atmos.DID("did:plc:quotasecond")
			firstKey, firstCAR, _, _, _ := buildChainBreak(t, firstDID)
			secondKey, secondCAR, cs, _, trigger := buildChainBreak(t, secondDID)
			firstSize := carPayloadBytes(t, firstCAR)
			secondSize := carPayloadBytes(t, secondCAR)
			limit := max(firstSize, secondSize) + min(firstSize, secondSize)/2

			xc := testutil.NewFakeSyncServerMulti(t, func(did atmos.DID) ([]byte, bool) {
				switch did {
				case firstDID:
					return firstCAR, true
				case secondDID:
					return secondCAR, true
				default:
					return nil, false
				}
			})
			entered, release := make(chan struct{}), make(chan struct{})
			tracking := testutil.NewTrackingResolver()
			tracking.Docs[firstDID] = testutil.BuildDIDDoc(firstDID, firstKey.PublicKey())
			tracking.Docs[secondDID] = testutil.BuildDIDDoc(secondDID, secondKey.PublicKey())
			dir := &identity.Directory{Resolver: gatedResyncResolver{tracking, firstDID, entered, release}}
			v, err := sync.NewVerifier(sync.VerifierOptions{
				Directory: dir, StateStore: cs, ResyncScratchBytes: gt.Some(limit),
				SyncClient: gt.Some(sync.NewClient(sync.Options{Client: xc, Directory: gt.Some(dir)})),
			})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, v.Close()) })

			want, err := cs.LoadChain(ctx, secondDID)
			require.NoError(t, err)
			fetchCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			firstDone := make(chan error, 1)
			go func() {
				_, err := v.Resync(fetchCtx, firstDID)
				firstDone <- err
			}()
			select {
			case <-entered:
			case err := <-firstDone:
				t.Fatalf("first resync completed before signature gate: %v", err)
			case <-time.After(5 * time.Second):
				t.Fatal("first resync did not reach signature gate")
			}

			ops, err := v.VerifyCommit(ctx, trigger())
			require.NoError(t, err)
			require.Nil(t, ops)
			select {
			case err := <-v.AsyncErrors():
				require.ErrorIs(t, err, repo.ErrCARSpillQuota)
			case result := <-v.ResyncEvents():
				t.Fatalf("unexpected resync event: %v", result.Reason)
			case <-time.After(5 * time.Second):
				t.Fatal("quota refusal timed out")
			}
			after, err := cs.LoadChain(ctx, secondDID)
			require.NoError(t, err)
			require.Equal(t, want, after)

			if outcome == "cancelled" {
				cancel()
			} else {
				close(release)
			}
			select {
			case err := <-firstDone:
				if outcome == "cancelled" {
					require.ErrorIs(t, err, context.Canceled)
				} else {
					require.NoError(t, err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("first resync did not exit")
			}
			ops, err = v.Resync(ctx, secondDID)
			require.NoError(t, err)
			require.Len(t, ops, 1)
			after, err = cs.LoadChain(ctx, secondDID)
			require.NoError(t, err)
			require.NotEqual(t, want, after)
		})
	}
}

func TestResyncDiskStore_QuotaReleasedAfterRejectedApply(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	did := atmos.DID("did:plc:quotarejected")
	_, archive, cs, dir, _ := buildChainBreak(t, did)
	_, xc := serveCAR(t, archive)
	v, err := sync.NewVerifier(sync.VerifierOptions{
		Directory: dir, StateStore: cs, ResyncDiskStore: gt.Some(true),
		ResyncScratchBytes: gt.Some(carPayloadBytes(t, archive)),
		SyncClient:         gt.Some(sync.NewClient(sync.Options{Client: xc, Directory: gt.Some(dir)})),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, v.Close()) })

	original, err := cs.LoadChain(ctx, did)
	require.NoError(t, err)
	require.NotNil(t, original)
	advanced := sync.ChainState{Rev: "zzzzzzzzzzzzz", Data: original.Data}
	require.NoError(t, cs.SaveChain(ctx, did, advanced))

	ops, err := v.Resync(ctx, did)
	require.Nil(t, ops)
	_, rejected := errors.AsType[*sync.RevRegressionError](err)
	require.True(t, rejected, "actual error: %v", err)
	after, err := cs.LoadChain(ctx, did)
	require.NoError(t, err)
	require.Equal(t, &advanced, after)

	require.NoError(t, cs.SaveChain(ctx, did, *original))
	ops, err = v.Resync(ctx, did)
	require.NoError(t, err)
	require.Len(t, ops, 1)
}
