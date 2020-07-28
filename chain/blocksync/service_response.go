package blocksync

import (
	"bufio"
	"context"
	"time"

	"go.opencensus.io/trace"
	"golang.org/x/xerrors"

	cborutil "github.com/filecoin-project/go-cbor-util"

	"github.com/filecoin-project/lotus/chain/store"
	"github.com/filecoin-project/lotus/chain/types"

	"github.com/ipfs/go-cid"
	inet "github.com/libp2p/go-libp2p-core/network"
)


// BlockSyncService is the component that services BlockSync requests from
// peers.
//
// BlockSync is the basic chain synchronization protocol of Filecoin. BlockSync
// is an RPC-oriented protocol, with a single operation to request blocks.
//
// A request contains a start anchor block (referred to with a CID), and a
// amount of blocks requested beyond the anchor (including the anchor itself).
//
// A client can also pass options, encoded as a 64-bit bitfield. Lotus supports
// two options at the moment:
//
//  - include block contents
//  - include block messages
//
// The response will include a status code, an optional message, and the
// response payload in case of success. The payload is a slice of serialized
// tipsets.
// FIXME: Rename to just `Service`.
type BlockSyncService struct {
	cs *store.ChainStore
}

func NewBlockSyncService(cs *store.ChainStore) *BlockSyncService {
	return &BlockSyncService{
		cs: cs,
	}
}

// Entry point of the service, handles `BlockSyncRequest`s.
func (bss *BlockSyncService) HandleStream(stream inet.Stream) {
	ctx, span := trace.StartSpan(context.Background(), "blocksync.HandleStream")
	defer span.End()

	defer stream.Close() //nolint:errcheck

	var req BlockSyncRequest
	if err := cborutil.ReadCborRPC(bufio.NewReader(stream), &req); err != nil {
		log.Warnf("failed to read block sync request: %s", err)
		return
	}
	log.Infow("block sync request",
		"start", req.Start, "len", req.RequestLength)

	resp, err := bss.processRequest(ctx, &req)
	if err != nil {
		log.Warn("failed to process block sync request: ", err)
		return
	}

	// FIXME: Extract this constant.
	writeDeadline := 60 * time.Second
	_ = stream.SetDeadline(time.Now().Add(writeDeadline)) // always use real time for socket/stream deadlines.
	if err := cborutil.WriteCborRPC(stream, resp); err != nil {
		log.Warnw("failed to write back response for handle stream",
			"err", err, "peer", stream.Conn().RemotePeer())
		return
	}
}

// `BlockSyncRequest` processed and validated to query the tipsets needed.
type validatedRequest struct {
	startTipset         types.TipSetKey
	count uint64
	options *BSOptions
}

// Validate and service the request. We return either a protocol
// response or an internal error. The protocol response may signal
// a protocol error itself (e.g., invalid request).
func (bss *BlockSyncService) processRequest(
	ctx context.Context,
	req *BlockSyncRequest,
) (*BlockSyncResponse, error) {
	validReq, errResponse := validateRequest(ctx, req)
	if errResponse != nil {
		// The request did not pass validation, return the response
		//  indicating it.
		return errResponse, nil
	}

	return bss.serviceRequest(ctx, validReq)
}

// Validate request and process it as:
// * parsed options.
// * limited count.
// * `TipSetKey` generated from CIDs.
// FIXME: Document in more length the validations.
// We either return a `validatedRequest` or a `BlockSyncResponse`
//  indicating the error why we can't process it. It does not have
//  any internal errors, it just signals protocol ones.
func validateRequest(
	ctx context.Context,
	req *BlockSyncRequest,
) (*validatedRequest, *BlockSyncResponse) {
	_, span := trace.StartSpan(ctx, "blocksync.ValidateRequest")
	defer span.End()

	validReq := validatedRequest{}

	validReq.options = ParseBSOptions(req.Options)

	// FIXME: Consider returning StatusBadRequest here.
	validReq.count = req.RequestLength
	if validReq.count > BlockSyncMaxRequestLength {
		log.Warnw("limiting blocksync request length",
			"orig", req.RequestLength)
		validReq.count = BlockSyncMaxRequestLength
	}

	if len(req.Start) == 0 {
		return nil, &BlockSyncResponse{
			Status:  StatusBadRequest,
			Message: "no cids given in blocksync request",
		}
	}
	validReq.startTipset = types.NewTipSetKey(req.Start...)

	// FIXME: Add it to the defer at the start.
	span.AddAttributes(
		trace.BoolAttribute("blocks", validReq.options.IncludeBlocks),
		trace.BoolAttribute("messages", validReq.options.IncludeMessages),
		trace.Int64Attribute("reqlen", int64(validReq.count)),
	)

	return &validReq, nil
}

func (bss *BlockSyncService) serviceRequest(
	ctx context.Context,
	req *validatedRequest,
	) (*BlockSyncResponse, error) {
	_, span := trace.StartSpan(ctx, "blocksync.ServiceRequest")
	defer span.End()

	// FIXME: Maybe just collapse with `collectChainSegment` here.
	chain, err := collectChainSegment(bss.cs, req)
	if err != nil {
		log.Warn("block sync request: collectChainSegment failed: ", err)
		return &BlockSyncResponse{
			Status:  StatusInternalError,
			Message: err.Error(),
		}, nil
	}

	status := StatusOK
	if len(chain) < int(req.count) {
		status = StatusPartial
	}

	return &BlockSyncResponse{
		Chain:  chain,
		Status: status,
	}, nil
}

func collectChainSegment(
	cs *store.ChainStore,
	req *validatedRequest,
	) ([]*BSTipSet, error) {
	var bstips []*BSTipSet
	// `TipSetKey` pointing to the tipset we already have and will scan
	//  its parents to continue the search backwards.
	cur := req.startTipset
	for {
		var bst BSTipSet
		ts, err := cs.LoadTipSet(cur)
		if err != nil {
			return nil, xerrors.Errorf("failed loading tipset %s: %w", cur, err)
		}

		if req.options.IncludeMessages {
			bmsgs, bmincl, smsgs, smincl, err := gatherMessages(cs, ts)
			if err != nil {
				return nil, xerrors.Errorf("gather messages failed: %w", err)
			}

			// FIXME: Pass the response to the function and set all this there.
			bst.BlsMessages = bmsgs
			bst.BlsMsgIncludes = bmincl
			bst.SecpkMessages = smsgs
			bst.SecpkMsgIncludes = smincl
		}

		if req.options.IncludeBlocks {
			bst.Blocks = ts.Blocks()
		}

		bstips = append(bstips, &bst)

		// If we collected the length requested or if we reached the
		// start (genesis), then stop.
		if uint64(len(bstips)) >= req.count || ts.Height() == 0 {
			return bstips, nil
		}

		cur = ts.Parents()
	}
}

// FIXME: DRY. Use messages interface.
func gatherMessages(cs *store.ChainStore, ts *types.TipSet) ([]*types.Message, [][]uint64, []*types.SignedMessage, [][]uint64, error) {
	blsmsgmap := make(map[cid.Cid]uint64)
	secpkmsgmap := make(map[cid.Cid]uint64)
	var secpkmsgs []*types.SignedMessage
	var blsmsgs []*types.Message
	var secpkincl, blsincl [][]uint64

	for _, block := range ts.Blocks() {
		bmsgs, smsgs, err := cs.MessagesForBlock(block)
		if err != nil {
			return nil, nil, nil, nil, err
		}

		// FIXME: What is going on here? Why do we keep track of this?
		bmi := make([]uint64, 0, len(bmsgs))
		for _, m := range bmsgs {
			i, ok := blsmsgmap[m.Cid()]
			if !ok {
				i = uint64(len(blsmsgs))
				blsmsgs = append(blsmsgs, m)
				blsmsgmap[m.Cid()] = i
			}

			bmi = append(bmi, i)
		}
		blsincl = append(blsincl, bmi)

		smi := make([]uint64, 0, len(smsgs))
		for _, m := range smsgs {
			i, ok := secpkmsgmap[m.Cid()]
			if !ok {
				i = uint64(len(secpkmsgs))
				secpkmsgs = append(secpkmsgs, m)
				secpkmsgmap[m.Cid()] = i
			}

			smi = append(smi, i)
		}
		secpkincl = append(secpkincl, smi)
	}

	return blsmsgs, blsincl, secpkmsgs, secpkincl, nil
}

func bstsToFullTipSet(bts *BSTipSet) (*store.FullTipSet, error) {
	fts := &store.FullTipSet{}
	for i, b := range bts.Blocks {
		fb := &types.FullBlock{
			Header: b,
		}
		for _, mi := range bts.BlsMsgIncludes[i] {
			fb.BlsMessages = append(fb.BlsMessages, bts.BlsMessages[mi])
		}
		for _, mi := range bts.SecpkMsgIncludes[i] {
			fb.SecpkMessages = append(fb.SecpkMessages, bts.SecpkMessages[mi])
		}

		fts.Blocks = append(fts.Blocks, fb)
	}

	return fts, nil
}
