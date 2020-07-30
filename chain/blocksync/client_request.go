package blocksync

import (
	"bufio"
	"context"
	"fmt"
	"math/rand"
	"time"

	bserv "github.com/ipfs/go-blockservice"
	graphsync "github.com/ipfs/go-graphsync"
	host "github.com/libp2p/go-libp2p-core/host"
	inet "github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"go.opencensus.io/trace"
	"golang.org/x/xerrors"

	cborutil "github.com/filecoin-project/go-cbor-util"
	"github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/chain/store"
	"github.com/filecoin-project/lotus/chain/types"
	incrt "github.com/filecoin-project/lotus/lib/increadtimeout"
	"github.com/filecoin-project/lotus/lib/peermgr"
	"github.com/filecoin-project/lotus/node/modules/dtypes"
)

// Block synchronization client.
// FIXME: Rename to just `Client`.
// FIXME: What's its API? What can we do with it? Not just request blocks.
type BlockSync struct {
	// FIXME: Why does the client access the *entire* service?
	//  We are only using the IPFS `BlockGetter` interface.
	bserv bserv.BlockService
	gsync graphsync.GraphExchange
	// Used to manage connection to our peers through the protocol.
	// FIXME: We should have a reduced set here, just initialized
	//  with our protocol ID, we shouldn't be able to open *any*
	//  connection.
	host  host.Host

	peerTracker *bsPeerTracker

	// FIXME: Do we even use this?
	peerMgr     *peermgr.PeerMgr
}

func NewBlockSyncClient(
	// FIXME: REMOVE THIS.
	bserv dtypes.ChainBlockService,
	h host.Host,
	pmgr peermgr.MaybePeerMgr,
	gs dtypes.Graphsync,
) *BlockSync {
	return &BlockSync{
		// FIXME: REMOVE.
		bserv:       bserv,
		host:        h,
		peerTracker: newPeerTracker(pmgr.Mgr),
		// FIXME: REMOVE.
		peerMgr:     pmgr.Mgr,
		gsync:       gs,
	}
}

// Internal single point of entry to the client request processing
// servicing the external-facing API. Currently we have 3 very
// heterogeneous services exposed:
// * GetBlocks:         BSOptBlocks
// * GetFullTipSet:     BSOptBlocks | BSOptMessages
// * GetChainMessages:                BSOptMessages
// This function handles all the different combinations of the available
// request options without disrupting external calls. In the future the
// consumers should be forced to use a more standardized service and
// adhere to a single API derived from this function.
func (client *BlockSync) doRequest(
	ctx context.Context,
	req *BlockSyncRequest,
	// FIXME: Document.
	singlePeer *peer.ID,
) (*ValidatedResponse, error) {
	var peers []peer.ID
	if singlePeer != nil {
		peers = []peer.ID{*singlePeer}
	} else {
		peers := client.getShuffledPeers()
		if len(peers) == 0 {
			return nil, xerrors.Errorf("GetBlocks failed: no peers available")
		}
	}

	// Try for each peer available, return on the first successful response.
	startTime := build.Clock.Now() // FIXME: Should we track time per peer instead?
	for _, peer := range peers {
		// FIXME: doing this synchronously isn't great, but fetching in parallel
		//  may not be a good idea either. Think about this more.
		select {
		case <-ctx.Done():
			return nil, xerrors.Errorf("doRequest failed: %w", ctx.Err())
		default:
		}

		// Send request, read response.
		res, err := client.sendRequestToPeer(ctx, peer, req)
		if err != nil {
			if !xerrors.Is(err, inet.ErrNoConn) {
				log.Warnf("doRequest failed for peer %s: %s",
					peer.String(), err)
			}
			continue
		}

		// Parse response answer.

		// Verify blocks.
		// FIXME: Is there something we can verify from the messages?

		// Process: extract blocks into the tipset, return that, which
		// can be consumed directly by GetBlocks.


		validRes, err := client.processResponse(req, res)
		if err != nil {
			log.Warnf("peer %s response failed: %s",
				peer.String(), err)
			continue
		}

		client.peerTracker.logGlobalSuccess(build.Clock.Since(startTime))
		// FIXME: Extract constant.
		client.host.ConnManager().TagPeer(peer, "bsync", 25)
		return validRes, nil

	}

	// FIXME: REport a separte failed for single peer error.
	return nil, xerrors.Errorf("GetBlocks failed with all peers")
}

// process: Validate and extract.
// Extract the chain from the response, creating one tipset at a time
// and verifying each is connected to the next (its parent).
// We are conflating status and validation errors here for simplicity in
//  the code. // FIXME: Review this.
// FIXME: We should penalize here (before returning) the peer. This should
//  have a separate validation function maybe.
// FIXME: Check request parent (that start matches first). Check connections.
func (client *BlockSync) processResponse(
	req *BlockSyncRequest,
	res *BlockSyncResponse,
) (validRes *ValidatedResponse, err error) {
	err = res.statusToError()
	if err != nil {
		return nil, xerrors.Errorf("status error: %s", err)
	}

	if len(res.Chain) == 0 {
		// FIXME: We assume here then that count > 1. That should be in the
		//  protocol.
		return nil, xerrors.Errorf("got no blocks in successful response")
	}
	if len(res.Chain) > int(req.RequestLength) {
		return nil, xerrors.Errorf("got longer response (%d) than requested (%d)",
			len(res.Chain), req.RequestLength)
	}

	options := parseOptions(req.Options)

	if options.IncludeBlocks {
		validRes.Tipsets = make([]*types.TipSet, req.RequestLength)
		// FIXME: Just do a single for to extract into tipsets, then
		//  another to check parents that ignores the first index (starts at 1).
		//  Avoid this head/parent and append artifacts.
		head, err := types.NewTipSet(res.Chain[0].Blocks)
		if err != nil {
			return nil, err
		}
		validRes.Tipsets[0] = head
		for i := 1; i < len(res.Chain); i++ {
			parent, err := types.NewTipSet(res.Chain[i].Blocks)
			if err != nil {
				return nil, err
			}

			if parent.IsParentOf(head) == false {
				return nil, fmt.Errorf("tipsets [%d-%d] are not connected",
					i-1, i)
				// FIXME: Give more information here, maybe CIDs.
			}

			validRes.Tipsets[i] = parent
			head = parent
		}
	}

	if options.IncludeMessages {
		validRes.Messages = make([]*CompactedMessages, req.RequestLength)
		for i, tipset := range res.Chain {
			validRes.Messages[i] = tipset.Messages
		}
		// FIXME: Any check we can do here? Does it depends on having blocks?
		//  IF the blocks are present check that they match indexes for toFullTipSets.
	}

	return validRes, nil
}

// GetBlocks fetches count blocks from the network, from the provided tipset
// *backwards*, returning as many tipsets as count.
//
// {hint/usage}: This is used by the Syncer during normal chain syncing and when
// resolving forks.
func (client *BlockSync) GetBlocks(
	ctx context.Context,
	tsk types.TipSetKey,
	count int,
) ([]*types.TipSet, error) {
	ctx, span := trace.StartSpan(ctx, "bsync.GetBlocks")
	defer span.End()
	if span.IsRecordingEvents() {
		span.AddAttributes(
			trace.StringAttribute("tipset", fmt.Sprint(tsk.Cids())),
			trace.Int64Attribute("count", int64(count)),
		)
	}

	req := &BlockSyncRequest{
		Start:         tsk.Cids(),
		RequestLength: uint64(count),
		Options:       BSOptBlocks,
	}

	validRes, err := client.doRequest(ctx, req, nil)
	if err != nil {
		return nil, err
	}

	return validRes.Tipsets, nil
}

func (client *BlockSync) GetFullTipSet(
	ctx context.Context,
	peer peer.ID,
	tsk types.TipSetKey,
) (*store.FullTipSet, error) {
	// TODO: round robin through these peers on error

	req := &BlockSyncRequest{
		Start:         tsk.Cids(),
		RequestLength: 1,
		Options:       BSOptBlocks | BSOptMessages,
	}

	validRes, err := client.doRequest(ctx, req, &peer)
	if err != nil {
		return nil, err
	}

	return validRes.toFullTipSets()[0], nil
	// If `doRequest` didn't fail we are guaranteed to have at least
	//  *one* tipset here, so it's safe to index directly.
}

func (client *BlockSync) GetChainMessages(
	ctx context.Context,
	// FIXME: Standard naming.
	h *types.TipSet,
	count uint64,
	) ([]*CompactedMessages, error) {
	ctx, span := trace.StartSpan(ctx, "GetChainMessages")
	defer span.End()

	req := &BlockSyncRequest{
		Start:         h.Cids(),
		RequestLength: count,
		Options:       BSOptMessages,
	}

	validRes, err := client.doRequest(ctx, req, nil)
	if err != nil {
		return nil, err
	}

	return validRes.Messages, nil
}

// Send a request to a peer. Write request in the stream and read the
// response back. We do not do any processing of the request/response
// here.
func (client *BlockSync) sendRequestToPeer(
	ctx context.Context,
	peer peer.ID,
	req *BlockSyncRequest,
) (_ *BlockSyncResponse, err error) {
	// Trace code.
	ctx, span := trace.StartSpan(ctx, "sendRequestToPeer")
	defer span.End()
	if span.IsRecordingEvents() {
		span.AddAttributes(
			trace.StringAttribute("peer", peer.Pretty()),
		)
	}
	defer func() {
		if err != nil {
			if span.IsRecordingEvents() {
				span.SetStatus(trace.Status{
					Code:    5,
					Message: err.Error(),
				})
			}
		}
	}()
	// -- TRACE --

	supp, err := client.host.Peerstore().SupportsProtocols(peer, BlockSyncProtocolID)
	if err != nil {
		return nil, xerrors.Errorf("failed to get protocols for peer: %w", err)
	}
	if len(supp) == 0 || supp[0] != BlockSyncProtocolID {
		return nil, xerrors.Errorf("peer %s does not support protocol %s",
			peer, BlockSyncProtocolID)
		// FIXME: `ProtoBook` should support a *single* protocol check that returns
		//  a bool instead of a list.
	}

	connectionStart := build.Clock.Now()

	// Open stream to peer.
	stream, err := client.host.NewStream(
		inet.WithNoDial(ctx, "should already have connection"),
		peer,
		BlockSyncProtocolID)
	if err != nil {
		client.RemovePeer(peer)
		return nil, xerrors.Errorf("failed to open stream to peer: %w", err)
	}

	// Write request.
	// FIXME: Extract deadline constant.
	_ = stream.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if err := cborutil.WriteCborRPC(stream, req); err != nil {
		_ = stream.SetWriteDeadline(time.Time{})
		// FIXME: What's the point of setting a blank deadline that won't time out?
		//  Is this our way of clearing the old one?
		client.peerTracker.logFailure(peer, build.Clock.Since(connectionStart))
		return nil, err
	}
	// FIXME: Same, why are we doing this again here?
	_ = stream.SetWriteDeadline(time.Time{})

	// Read response.
	var res BlockSyncResponse
	err = cborutil.ReadCborRPC(
		// FIXME: Extract constants.
		bufio.NewReader(incrt.New(stream, 50<<10, 5*time.Second)),
		&res)
	if err != nil {
		client.peerTracker.logFailure(peer, build.Clock.Since(connectionStart))
		return nil, err
	}

	// FIXME: Move all this together at the top using a defer as done elsewhere.
	//  Maybe we need to declare `res` in the signature.
	if span.IsRecordingEvents() {
		span.AddAttributes(
			trace.Int64Attribute("resp_status", int64(res.Status)),
			trace.StringAttribute("msg", res.ErrorMessage),
			trace.Int64Attribute("chain_len", int64(len(res.Chain))),
		)
	}

	client.peerTracker.logSuccess(peer, build.Clock.Since(connectionStart))
	return &res, nil
}

func (client *BlockSync) AddPeer(p peer.ID) {
	client.peerTracker.addPeer(p)
}

func (client *BlockSync) RemovePeer(p peer.ID) {
	client.peerTracker.removePeer(p)
}

// getShuffledPeers returns a preference-sorted set of peers (by latency
// and failure counting), shuffling the first few peers so we don't always
// pick the same peer.
// FIXME: Merge with the shuffle if we *always* do it.
func (client *BlockSync) getShuffledPeers() []peer.ID {
	peers := client.peerTracker.prefSortedPeers()
	shufflePrefix(peers)
	return peers
}

func shufflePrefix(peers []peer.ID) {
	// FIXME: Extract.
	prefix := 5
	if len(peers) < prefix {
		prefix = len(peers)
	}

	buf := make([]peer.ID, prefix)
	perm := rand.Perm(prefix)
	for i, v := range perm {
		buf[i] = peers[v]
	}

	copy(peers, buf)
}
