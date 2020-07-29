package blocksync

import (
	"context"

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

func ParseBSOptions(optfield uint64) *BSOptions {
	return &BSOptions{
		IncludeBlocks:   optfield&(BSOptBlocks) != 0,
		IncludeMessages: optfield&(BSOptMessages) != 0,
	}
}

// FIXME: Type these.
const (
	// FIXME: This seems like it should be implicit in the protocol,
	// is there a case we don't want the blocks? The messages are
	//  not (cannot) be transported alone.
	BSOptBlocks = 1 << iota
	BSOptMessages
)

type BlockSyncResponse struct {
	Chain []*BSTipSet

	Status  uint64
	Message string
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

// FIXME: BlockSync representation of a tipset?
// FIXME: Use the chain message abstraction and do not repeat so much code.
type BSTipSet struct {
	Blocks []*types.BlockHeader

	BlsMessages    []*types.Message
	// FIXME: What is this?
	//  Is this just to map messages to blocks? Seems so, clean and document.
	BlsMsgIncludes [][]uint64

	SecpkMessages    []*types.SignedMessage
	SecpkMsgIncludes [][]uint64
}
