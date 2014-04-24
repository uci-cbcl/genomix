package edu.uci.ics.genomix.pregelix.operator.removebadcoverage;

import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;

import edu.uci.ics.genomix.data.config.GenomixJobConf;
import edu.uci.ics.genomix.data.types.EDGETYPE;
import edu.uci.ics.genomix.data.types.VKmer;
import edu.uci.ics.genomix.pregelix.base.DeBruijnGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.base.MessageWritable;
import edu.uci.ics.genomix.pregelix.base.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.base.VertexValueWritable.State;

/**
 * Graph clean pattern: Remove Badcoverage
 * Details: Chimeric reads and other sequencing artifacts create edges that are
 * only supported by a small number of reads. Besides that, there are some nodes 
 * with a very high coverage because of repeats. These edges/nodes are identified
 * and removed. This is then followed by recompressing the graph. 
 * 
 */
public class RemoveBadCoverageVertex extends DeBruijnGraphCleanVertex<VertexValueWritable, MessageWritable> {
    private static float minAverageCoverage = -1;
    private static float maxAverageCoverage = -1;

    @Override
    public void configure(Configuration conf) {
    	if (minAverageCoverage < 0) {
            minAverageCoverage = Float.parseFloat(getContext().getConfiguration().get(GenomixJobConf.REMOVE_BAD_COVERAGE_MIN_COVERAGE));
    	}
        if(maxAverageCoverage < 0) {
        	maxAverageCoverage = Float.parseFloat(getContext().getConfiguration().get(GenomixJobConf.REMOVE_BAD_COVERAGE_MAX_COVERAGE));
        }
        if (outgoingMsg == null) {
            outgoingMsg = new MessageWritable();
        }
    }

    @Override
    public void compute(Iterator<MessageWritable> msgIterator) {
    	VertexValueWritable vertex = getVertexValue();
        if (getSuperstep() == 1) {
            if (vertex.getAverageCoverage() < minAverageCoverage || vertex.getAverageCoverage() > maxAverageCoverage) {
            	for (EDGETYPE et : EDGETYPE.values) {
                    for (VKmer dest : vertex.getEdges(et)) {
                        outgoingMsg.reset();
                        outgoingMsg.setFlag(et.mirror().get());
                        outgoingMsg.setSourceVertexId(getVertexId());
                        sendMsg(dest, outgoingMsg);
                    }
            	}
                deleteVertex(getVertexId());
            }
        } else {
            MessageWritable incomingMsg;
            while (msgIterator.hasNext()) {
                incomingMsg = msgIterator.next();
                vertex.getEdges(EDGETYPE.fromByte(incomingMsg.getFlag())).remove(incomingMsg.getSourceVertexId());
            }
        }
        voteToHalt();
    }
}
