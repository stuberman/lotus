package blocksync

import (
	"bufio"
	"context"
	"fmt"
	"math/rand"
	"time"

	bserv "github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	graphsync "github.com/ipfs/go-graphsync"
	gsnet "github.com/ipfs/go-graphsync/network"
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

	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	ipldselector "github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipfs/go-datastore"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	selectorbuilder "github.com/ipld/go-ipld-prime/traversal/selector/builder"
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
		bserv:       bserv,
		host:        h,
		peerTracker: newPeerTracker(pmgr.Mgr),
		peerMgr:     pmgr.Mgr,
		gsync:       gs,
	}
}

// FIXME: Check request.
// FIXME: This error strings are repeated elsewhere.
// FIXME: Should be in the common file.
func (client *BlockSync) processStatus(req *BlockSyncRequest, res *BlockSyncResponse) error {
	switch res.Status {
	case StatusPartial: // Partial Response
		return xerrors.Errorf("not handling partial blocksync responses yet")
	case StatusNotFound: // req.Start not found
		return xerrors.Errorf("not found")
	case StatusGoAway: // Go Away
		return xerrors.Errorf("not handling 'go away' blocksync responses yet")
	case StatusInternalError: // Internal Error
		return xerrors.Errorf("block sync peer errored: %s", res.Message)
	case StatusBadRequest:
		return xerrors.Errorf("block sync request invalid: %s", res.Message)
	default:
		return xerrors.Errorf("unrecognized response code: %d", res.Status)
	}
}

// GetBlocks fetches count blocks from the network, from the provided tipset
// *backwards*, returning as many tipsets as count.
//
// {hint/usage}: This is used by the Syncer during normal chain syncing and when
// resolving forks.
func (client *BlockSync) GetBlocks(ctx context.Context, tsk types.TipSetKey, count int) ([]*types.TipSet, error) {
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

	// this peerset is sorted by latency and failure counting.
	peers := client.getPeers()
	// randomize the first few peers so we don't always pick the same peer
	// FIXME: This pattern is repeated, encapsulate into `getShuffledPeers`,
	//  or inside getPeers.
	shufflePrefix(peers)

	startTime := build.Clock.Now()
	var anyError error

	for _, peer := range peers {
		// FIXME: doing this synchronously isn't great, but fetching in parallel
		//  may not be a good idea either. Think about this more.
		select {
		case <-ctx.Done():
			return nil, xerrors.Errorf("GetBlocks failed: %w", ctx.Err())
		default:
		}

		res, err := client.sendRequestToPeer(ctx, peer, req)
		if err != nil {
			// FIXME: Overwriting errors.
			anyError = err
			if !xerrors.Is(err, inet.ErrNoConn) {
				log.Warnf("BlockSync request failed for peer %s: %s",
					peer.String(), err)
			}
			continue
		}

		if res.Status == StatusOK || res.Status == StatusPartial {
			// FIXME: The status check probably should be part of `processBlocksResponse`.
			resp, err := client.processBlocksResponse(req, res)
			// FIXME: Differentiate res vs resp (the second is actually
			//  the extracted tipsets). Process should be validate and
			//  extract.
			if err != nil {
				return nil, xerrors.Errorf("processBlocksResponse failed: %w",
					err)
			}
			client.peerTracker.logGlobalSuccess(build.Clock.Since(startTime))
			// FIXME: Extract constant.
			client.host.ConnManager().TagPeer(peer, "bsync", 25)
			return resp, nil
		}

		// FIXME: Why is this disconnected from the above?
		anyError = client.processStatus(req, res)
		if anyError != nil {
			log.Warnf("BlockSync peer %s response was an error: %s",
				peer.String(), anyError)
		}
	}

	return nil, xerrors.Errorf("GetBlocks failed with all peers: %w", anyError)
}

// FIXME: Reuse from `GetBlocks` once that function is reviewed.
func (client *BlockSync) GetFullTipSet(ctx context.Context, p peer.ID, tsk types.TipSetKey) (*store.FullTipSet, error) {
	// TODO: round robin through these peers on error

	req := &BlockSyncRequest{
		Start:         tsk.Cids(),
		RequestLength: 1,
		Options:       BSOptBlocks | BSOptMessages,
	}

	res, err := client.sendRequestToPeer(ctx, p, req)
	if err != nil {
		return nil, err
	}

	// FIXME: USE CONSTANTS!
	switch res.Status {
	case 0: // Success
		if len(res.Chain) == 0 {
			return nil, fmt.Errorf("got zero length chain response")
		}
		bts := res.Chain[0]

		return bstsToFullTipSet(bts)
	case 101: // Partial Response
		return nil, xerrors.Errorf("partial responses are not handled for single tipset fetching")
	case 201: // req.Start not found
		return nil, fmt.Errorf("not found")
	case 202: // Go Away
		return nil, xerrors.Errorf("received 'go away' response peer")
	case 203: // Internal Error
		return nil, fmt.Errorf("block sync peer errored: %q", res.Message)
	case 204: // Invalid Request
		return nil, fmt.Errorf("block sync request invalid: %q", res.Message)
	default:
		return nil, fmt.Errorf("unrecognized response code")
	}
}

func shufflePrefix(peers []peer.ID) {
	// FIXME: Extract.
	pref := 5
	if len(peers) < pref {
		pref = len(peers)
	}

	buf := make([]peer.ID, pref)
	perm := rand.Perm(pref)
	for i, v := range perm {
		buf[i] = peers[v]
	}

	copy(peers, buf)
}

// FIXME: Reuse from `GetBlocks` once that function is reviewed.
//  What is exactly the difference between the two? Just `BSOptMessages`
//  versus `BSOptBlocks`?
func (client *BlockSync) GetChainMessages(
	ctx context.Context,
	// FIXME: Standard naming.
	h *types.TipSet,
	count uint64,
	) ([]*BSTipSet, error) {
	ctx, span := trace.StartSpan(ctx, "GetChainMessages")
	defer span.End()

	peers := client.getPeers()
	// randomize the first few peers so we don't always pick the same peer
	shufflePrefix(peers)

	// FIXME: Same from GetBlocks.
	req := &BlockSyncRequest{
		Start:         h.Cids(),
		RequestLength: count,
		Options:       BSOptMessages,
	}

	var err error
	start := build.Clock.Now()

	for _, p := range peers {
		res, rerr := client.sendRequestToPeer(ctx, p, req)
		if rerr != nil {
			err = rerr
			log.Warnf("BlockSync request failed for peer %s: %s", p.String(), err)
			continue
		}

		if res.Status == StatusOK {
			client.peerTracker.logGlobalSuccess(build.Clock.Since(start))
			return res.Chain, nil
		}

		if res.Status == StatusPartial {
			// TODO: track partial response sizes to ensure we don't overrequest too often
			return res.Chain, nil
		}

		err = client.processStatus(req, res)
		if err != nil {
			log.Warnf("BlockSync peer %s response was an error: %s", p.String(), err)
		}
	}

	if err == nil {
		// FIXME: Is `no peers connected` the only possible reason of a
		//  problem here?
		return nil, xerrors.Errorf("GetChainMessages failed, no peers connected")
	}

	// TODO: What if we have no peers (and err is nil)?
	return nil, xerrors.Errorf("GetChainMessages failed with all peers(%d): %w", len(peers), err)
}

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

	gsproto := string(gsnet.ProtocolGraphsync)
	supp, err := client.host.Peerstore().SupportsProtocols(peer, BlockSyncProtocolID, gsproto)
	if err != nil {
		return nil, xerrors.Errorf("failed to get protocols for peer: %w", err)
	}
	if len(supp) == 0 {
		return nil, xerrors.Errorf("peer %s supports no known sync protocols", peer)
	}

	// FIXME: Shouldn't we check all of them?
	switch supp[0] {
	case BlockSyncProtocolID:
		res, err := client.fetchBlocksBlockSync(ctx, peer, req)
		if err != nil {
			return nil, xerrors.Errorf("blocksync req failed: %w", err)
		}
		return res, nil
	case gsproto:
		res, err := client.fetchBlocksGraphSync(ctx, peer, req)
		if err != nil {
			return nil, xerrors.Errorf("graphsync req failed: %w", err)
		}
		return res, nil
	default:
		// FIXME: Just the first one, we don't check all.
		return nil, xerrors.Errorf("peerstore somehow returned unexpected protocols: %v", supp)
	}

}

// FIXME: Rename, we don't fetch anything here, just read the response
//  of the request. Might be worth merging with `sendRequestToPeer` once
//  that is cleaned up.
func (client *BlockSync) fetchBlocksBlockSync(
	ctx context.Context,
	peer peer.ID,
	req *BlockSyncRequest,
) (*BlockSyncResponse, error) {
	ctx, span := trace.StartSpan(ctx, "blockSyncFetch")
	defer span.End()

	start := build.Clock.Now()
	stream, err := client.host.NewStream(
		inet.WithNoDial(ctx, "should already have connection"),
		peer,
		BlockSyncProtocolID)
	if err != nil {
		client.RemovePeer(peer)
		return nil, xerrors.Errorf("failed to open stream to peer: %w", err)
	}
	// FIXME: Extract deadline constant.
	_ = stream.SetWriteDeadline(time.Now().Add(5 * time.Second)) // always use real time for socket/stream deadlines.

	if err := cborutil.WriteCborRPC(stream, req); err != nil {
		// FIXME: What's the point of setting a blank deadline that won't time out?
		_ = stream.SetWriteDeadline(time.Time{})
		client.peerTracker.logFailure(peer, build.Clock.Since(start))
		return nil, err
	}
	// FIXME: Same. Why are we doing this?
	_ = stream.SetWriteDeadline(time.Time{})

	var res BlockSyncResponse
	err = cborutil.ReadCborRPC(
		// FIXME: Extract constants.
		bufio.NewReader(incrt.New(stream, 50<<10, 5*time.Second)),
		&res)
	if err != nil {
		client.peerTracker.logFailure(peer, build.Clock.Since(start))
		return nil, err
	}
	client.peerTracker.logSuccess(peer, build.Clock.Since(start))

	// FIXME: Move all this together with a defer as elsewhere. Maybe
	//  we need to declare `res` in the signature.
	if span.IsRecordingEvents() {
		span.AddAttributes(
			trace.Int64Attribute("resp_status", int64(res.Status)),
			trace.StringAttribute("msg", res.Message),
			trace.Int64Attribute("chain_len", int64(len(res.Chain))),
		)
	}

	return &res, nil
}

// FIXME: Check request.
// FIXME: Rename to just `processResponse`. Similar to the service model,
//  validate and service (or equivalent, in this case maybe store response).
func (client *BlockSync) processBlocksResponse(
	req *BlockSyncRequest,
	res *BlockSyncResponse,
) ([]*types.TipSet, error) {
	if len(res.Chain) == 0 {
		return nil, xerrors.Errorf("got no blocks in successful blocksync response")
	}

	// FIXME: Comment on current/next.
	cur, err := types.NewTipSet(res.Chain[0].Blocks)
	if err != nil {
		return nil, err
	}

	// FIXME: REVIEW all this logic.
	out := []*types.TipSet{cur}
	for bi := 1; bi < len(res.Chain); bi++ {
		next := res.Chain[bi].Blocks
		nts, err := types.NewTipSet(next)
		if err != nil {
			return nil, err
		}

		if !types.CidArrsEqual(cur.Parents().Cids(), nts.Cids()) {
			return nil, fmt.Errorf("parents of tipset[%d] were not tipset[%d]",
				bi-1, bi)
		}

		out = append(out, nts)
		cur = nts
	}
	return out, nil
}

// FIXME: Who uses this? Remove otherwise.
func (client *BlockSync) GetBlock(ctx context.Context, c cid.Cid) (*types.BlockHeader, error) {
	sb, err := client.bserv.GetBlock(ctx, c)
	if err != nil {
		return nil, err
	}

	return types.DecodeBlock(sb.RawData())
}

func (client *BlockSync) AddPeer(p peer.ID) {
	client.peerTracker.addPeer(p)
}

func (client *BlockSync) RemovePeer(p peer.ID) {
	client.peerTracker.removePeer(p)
}

// getPeers returns a preference-sorted set of peers to query.
// FIXME: Merge with the shuffle if we *always* do it.
func (client *BlockSync) getPeers() []peer.ID {
	return client.peerTracker.prefSortedPeers()
}

const (

	// AMT selector recursion. An AMT has arity of 8 so this gives allows
	// us to retrieve trees with 8^10 (1,073,741,824) elements.
	amtRecursionDepth = uint32(10)

	// some constants for looking up tuple encoded struct fields
	// field index of Parents field in a block header
	blockIndexParentsField = 5

	// field index of Messages field in a block header
	blockIndexMessagesField = 10

	// field index of AMT node in AMT head
	amtHeadNodeFieldIndex = 2

	// field index of links array AMT node
	amtNodeLinksFieldIndex = 1

	// field index of values array AMT node
	amtNodeValuesFieldIndex = 2

	// maximum depth per traversal
	maxRequestLength = 50
)

var amtSelector selectorbuilder.SelectorSpec

func init() {
	// builer for selectors
	ssb := selectorbuilder.NewSelectorSpecBuilder(basicnode.Style.Any)
	// amt selector -- needed to selector through a messages AMT
	amtSelector = ssb.ExploreIndex(amtHeadNodeFieldIndex,
		ssb.ExploreRecursive(ipldselector.RecursionLimitDepth(int(amtRecursionDepth)),
			ssb.ExploreUnion(
				ssb.ExploreIndex(amtNodeLinksFieldIndex,
					ssb.ExploreAll(ssb.ExploreRecursiveEdge())),
				ssb.ExploreIndex(amtNodeValuesFieldIndex,
					ssb.ExploreAll(ssb.Matcher())))))
}

func selectorForRequest(req *BlockSyncRequest) ipld.Node {
	// builer for selectors
	ssb := selectorbuilder.NewSelectorSpecBuilder(basicnode.Style.Any)

	bso := ParseBSOptions(req.Options)
	if bso.IncludeMessages {
		return ssb.ExploreRecursive(ipldselector.RecursionLimitDepth(int(req.RequestLength)),
			ssb.ExploreIndex(blockIndexParentsField,
				ssb.ExploreUnion(
					ssb.ExploreAll(
						ssb.ExploreIndex(blockIndexMessagesField,
							ssb.ExploreRange(0, 2, amtSelector),
						)),
					ssb.ExploreIndex(0, ssb.ExploreRecursiveEdge()),
				))).Node()
	}
	return ssb.ExploreRecursive(ipldselector.RecursionLimitDepth(int(req.RequestLength)), ssb.ExploreIndex(blockIndexParentsField,
		ssb.ExploreUnion(
			ssb.ExploreAll(
				ssb.Matcher(),
			),
			ssb.ExploreIndex(0, ssb.ExploreRecursiveEdge()),
		))).Node()
}

func firstTipsetSelector(req *BlockSyncRequest) ipld.Node {
	// builer for selectors
	ssb := selectorbuilder.NewSelectorSpecBuilder(basicnode.Style.Any)

	bso := ParseBSOptions(req.Options)
	if bso.IncludeMessages {
		return ssb.ExploreIndex(blockIndexMessagesField,
			ssb.ExploreRange(0, 2, amtSelector),
		).Node()
	}
	return ssb.Matcher().Node()

}

func (client *BlockSync) executeGsyncSelector(ctx context.Context, p peer.ID, root cid.Cid, sel ipld.Node) error {
	extension := graphsync.ExtensionData{
		Name: "chainsync",
		Data: nil,
	}
	_, errs := client.gsync.Request(ctx, p, cidlink.Link{Cid: root}, sel, extension)

	for err := range errs {
		return xerrors.Errorf("failed to complete graphsync request: %w", err)
	}
	return nil
}

// Fallback for interacting with other non-lotus nodes
func (client *BlockSync) fetchBlocksGraphSync(ctx context.Context, p peer.ID, req *BlockSyncRequest) (*BlockSyncResponse, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	immediateTsSelector := firstTipsetSelector(req)

	// Do this because we can only request one root at a time
	for _, r := range req.Start {
		if err := client.executeGsyncSelector(ctx, p, r, immediateTsSelector); err != nil {
			return nil, err
		}
	}

	if req.RequestLength > maxRequestLength {
		req.RequestLength = maxRequestLength
	}

	sel := selectorForRequest(req)

	// execute the selector forreal
	if err := client.executeGsyncSelector(ctx, p, req.Start[0], sel); err != nil {
		return nil, err
	}

	// Now pull the data we fetched out of the chainstore (where it should now be persisted)
	tempcs := store.NewChainStore(client.bserv.Blockstore(), datastore.NewMapDatastore(), nil)

	validReq, errResponse := validateRequest(ctx, req)
	if errResponse != nil {
		return errResponse, nil
	}

	chain, err := collectChainSegment(tempcs, validReq)
	if err != nil {
		return nil, xerrors.Errorf("failed to load chain data from chainstore after successful graphsync response (start = %v): %w", req.Start, err)
	}

	return &BlockSyncResponse{Chain: chain}, nil
}
