package edu.uci.ics.genomix.pregelix.operator.removebadcoverage;

import java.util.Iterator;

import edu.uci.ics.genomix.data.config.GenomixJobConf;
import edu.uci.ics.genomix.data.types.Kmer;
import edu.uci.ics.genomix.data.types.KmerFactory;
import edu.uci.ics.genomix.data.types.ReadHeadInfo;
import edu.uci.ics.genomix.data.types.ReadHeadSet;
import edu.uci.ics.genomix.data.types.VKmer;
import edu.uci.ics.genomix.pregelix.base.DeBruijnGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.base.VertexValueWritable;

public class ShiftLowCoverageReadSetVertex extends DeBruijnGraphCleanVertex<VertexValueWritable, ShiftedReadSetMessage> {

    protected static float minAverageCoverage = -1;

    /**
     * initiate kmerSize, length
     */
    @Override
    public void initVertex() {
        super.initVertex();
        if (minAverageCoverage < 0)
            minAverageCoverage = Float.parseFloat(getContext().getConfiguration().get(
                    GenomixJobConf.REMOVE_BAD_COVERAGE_MIN_COVERAGE));
        if (outgoingMsg == null)
            outgoingMsg = new ShiftedReadSetMessage();
        ReadHeadSet.forceWriteEntireBody(true);
    }

    private boolean isLowCoverageVertex() {
        VertexValueWritable vertex = getVertexValue();
        return vertex.getAverageCoverage() <= minAverageCoverage;
    }

    private boolean hasReadSet() {
        VertexValueWritable vertex = getVertexValue();
        return vertex.hasUnflippedOrFlippedReadIds();
    }

    @Override
    public void compute(Iterator<ShiftedReadSetMessage> msgIterator) throws Exception {
        if (super.getSuperstep() > 1) {
            acceptNewReadSet(msgIterator);
        }
        if (isLowCoverageVertex() && hasReadSet()) {
            LOG.info("Here is one head: " + getVertexId());
            shiftReadSetToNeighors();
            clearupMyReadSet();
        }
        super.voteToHalt();
    }

    private void clearupMyReadSet() {
        getVertexValue().getUnflippedReadIds().reset();
        getVertexValue().getFlippedReadIds().reset();
    }

    private void shiftReadSetToNeighors() {
        ReadHeadSet unflippedSet = getVertexValue().getUnflippedReadIds();
        KmerFactory kmerFactory = new KmerFactory(Kmer.getKmerLength());
        for (ReadHeadInfo info : unflippedSet) {
            if (info.getOffset() > 0) {
                throw new IllegalStateException("the info's offset should be negative, now:" + info.getOffset());
            }
            VKmer destForward = kmerFactory.getSubKmerFromChain(-info.getOffset() + 1, Kmer.getKmerLength(),
                    info.getThisReadSequence());
            if (destForward == null) {
                continue;
            }

            VKmer destReverse = destForward.reverse();
            boolean flipped = destForward.compareTo(destReverse) > 0;

            if (flipped) {
                // -->      if this is 0
                //  <--     then neighbor should be 3
                // ----->
                info.resetOffset(-info.getOffset() + destReverse.getKmerLetterLength());
                sendReadInfo(destReverse, flipped, info);
            } else {
                // -->      if this is 0
                //  -->     then neighbor should be -1
                // ------>
                info.resetOffset(info.getOffset() - 1);
                sendReadInfo(destForward, flipped, info);
            }

        }

        ReadHeadSet flippedSet = getVertexValue().getFlippedReadIds();
        for (ReadHeadInfo info : flippedSet) {
            if (info.getOffset() < Kmer.getKmerLength() - 1) {
                throw new IllegalStateException("the info's offset should be >= than the kmerlength-1, now:"
                        + info.getOffset());
            }
            // <--     
            //    ----->
            VKmer destForward = kmerFactory.getSubKmerFromChain(info.getOffset() - (Kmer.getKmerLength() - 1) + 1,
                    Kmer.getKmerLength(), info.getThisReadSequence());
            if (destForward == null) {
                continue;
            }
            VKmer destReverse = destForward.reverse();
            boolean flipped = destForward.compareTo(destReverse) > 0;

            if (flipped) {
                // <--      if this is 2
                //  <--     then the next neighbor should be 3
                //     ------>
                info.resetOffset(info.getOffset() + 1);
                sendReadInfo(destReverse, flipped, info);
            } else {
                // <--              if this is 3
                //       -->        then the next neighbor should be -2         
                //     ------->
                info.resetOffset(-(info.getOffset() + 1 - Kmer.getKmerLength() + 1));
                sendReadInfo(destForward, flipped, info);
            }
        }
    }

    private void sendReadInfo(VKmer dest, boolean flipped, ReadHeadInfo info) {
        outFlag = (short) (flipped ? 1 : 0);
        outgoingMsg.reset();
        outgoingMsg.setFlag(outFlag);
        outgoingMsg.setSourceVertexId(getVertexId());
        outgoingMsg.setReadInfo(info);
        sendMsg(dest, outgoingMsg);
    }

    private void acceptNewReadSet(Iterator<ShiftedReadSetMessage> msgIterator) {
        VertexValueWritable vertex = getVertexValue();
        while (msgIterator.hasNext()) {
            ShiftedReadSetMessage msg = msgIterator.next();
            if (msg.getFlag() > 0) { // flipped
                vertex.getFlippedReadIds().add(msg.getReadInfo());
            } else {
                vertex.getUnflippedReadIds().add(msg.getReadInfo());
            }
        }
    }

}
