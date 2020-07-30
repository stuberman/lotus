package blocksync

import (
	"context"
	"github.com/filecoin-project/lotus/chain/store"

	"golang.org/x/xerrors"
	"github.com/libp2p/go-libp2p-core/peer"
	logging "github.com/ipfs/go-log"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/protocol"
	"github.com/ipfs/go-cid"

	"github.com/filecoin-project/lotus/chain/types"
)

var log = logging.Logger("blocksync")

// FIXME: Check who uses this
type NewStreamFunc func(context.Context, peer.ID, ...protocol.ID) (network.Stream, error)

const BlockSyncProtocolID = "/fil/sync/blk/0.0.1"

const BlockSyncMaxRequestLength = 800


type BlockSyncRequest struct {
	// List of CIDs comprising a `TipSetKey` from where to start fetching.
	// FIXME: Why don't we send a `TipSetKey` instead of converting back
	//  and forth?
	Start         []cid.Cid
	// Number of block sets to fetch from `Start`.
	// FIXME: Including start?
	// FIXME: Rename, remove `Request` and change Length to `Count`
	//  or similar.
	RequestLength uint64

	Options uint64
}

type BSOptions struct {
	IncludeBlocks   bool
	IncludeMessages bool
}

func parseOptions(optfield uint64) *BSOptions {
	return &BSOptions{
		IncludeBlocks:   optfield&(BSOptBlocks) != 0,
		IncludeMessages: optfield&(BSOptMessages) != 0,
	}
}

type BlockSyncResponse struct {
	Chain []*BSTipSet

	Status       uint64
	ErrorMessage string
}

const (
	StatusOK            = uint64(0)
	// We could not fetch all blocks requested.
	StatusPartial       = uint64(101)
	StatusNotFound      = uint64(201)
	StatusGoAway        = uint64(202)
	StatusInternalError = uint64(203)
	StatusBadRequest    = uint64(204)
)

// FIXME: Type these.
const (
	// FIXME: This seems like it should be implicit in the protocol,
	// is there a case we don't want the blocks? The messages are
	//  not (cannot) be transported alone. GetChainMessages does not
	//  use it.
	// FIXME: Rename to tipsets or block headers
	BSOptBlocks = 1 << iota
	BSOptMessages
)

// Convert status to internal error.
// FIXME: Check request.
// FIXME: This error strings are repeated elsewhere.
// FIXME: Should be in the common file.
func (res *BlockSyncResponse) statusToError() error {
	switch res.Status {
	case StatusOK, StatusPartial:
		// FIXME: Consider if we want to not process `StatusPartial`
		//  and return an error.
		return nil
	case StatusNotFound:
		return xerrors.Errorf("not found")
	case StatusGoAway:
		return xerrors.Errorf("not handling 'go away' blocksync responses yet")
	case StatusInternalError:
		return xerrors.Errorf("block sync peer errored: %s", res.ErrorMessage)
	case StatusBadRequest:
		return xerrors.Errorf("block sync request invalid: %s", res.ErrorMessage)
	default:
		return xerrors.Errorf("unrecognized response code: %d", res.Status)
	}
}

// FIXME: BlockSync representation of a tipset?
// FIXME: Use the chain message abstraction and do not repeat so much code.
type BSTipSet struct {
	Blocks []*types.BlockHeader
    Messages *CompactedMessages
}

// FIXME: Describe format.
// FIXME: The logic of this function should belong to it, not
//  to the consumer.
type CompactedMessages struct {
	Bls    []*types.Message
	BlsIncludes [][]uint64

	Secpk    []*types.SignedMessage
	SecpkIncludes [][]uint64
}

type ValidatedResponse struct {
	// Only the BSTipSet with blocks and messages.
	Tipsets []*types.TipSet

	Messages []*CompactedMessages
}

// Decompress messages and form full tipsets.
func (res *ValidatedResponse) toFullTipSets() ([]*store.FullTipSet) {
	if len(res.Tipsets) == 0 {
		// This decompression can only be done if both blocks and
		// messages are returned in the response.
		// FIXME: Do we need to check the messages are present also?
		return nil
	}
	ftsList := make([]*store.FullTipSet, len(res.Tipsets))
	for tipsetIdx := range res.Tipsets {
		fts := &store.FullTipSet{} // FIXME: We should use the `NewFullTipSet` API.
		msgs := res.Messages[tipsetIdx]
		for blockIdx, b := range res.Tipsets[tipsetIdx].Blocks() {
			fb := &types.FullBlock{
				Header: b,
			}
			for _, mi := range msgs.BlsIncludes[blockIdx] {
				fb.BlsMessages = append(fb.BlsMessages, msgs.Bls[mi])
			}
			for _, mi := range msgs.SecpkIncludes[blockIdx] {
				fb.SecpkMessages = append(fb.SecpkMessages, msgs.Secpk[mi])
			}

			fts.Blocks = append(fts.Blocks, fb)
		}
		ftsList[tipsetIdx] = fts
	}
	return ftsList
}
