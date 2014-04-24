package edu.uci.ics.genomix.pregelix.operator.seeddetection;

import java.util.Iterator;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configuration;

import edu.uci.ics.genomix.data.config.GenomixJobConf;
import edu.uci.ics.genomix.data.types.DIR;
import edu.uci.ics.genomix.data.types.EDGETYPE;
import edu.uci.ics.genomix.data.types.VKmer;
import edu.uci.ics.genomix.pregelix.base.DeBruijnGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.base.MessageWritable;
import edu.uci.ics.genomix.pregelix.base.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.operator.scaffolding2.NodeToRayVertexInputFormat;
import edu.uci.ics.genomix.pregelix.operator.scaffolding2.RayMessage;
import edu.uci.ics.genomix.pregelix.operator.scaffolding2.RayVertexToNodeOutputFormat;
import edu.uci.ics.pregelix.api.job.PregelixJob;

public class SeedRetrievalVertex extends DeBruijnGraphCleanVertex<VertexValueWritable, SeedRetrievalMessage>{

	private static final Integer COVERAGE_THRESHOLD = 50;
	@Override
	public void compute(Iterator<SeedRetrievalMessage> msgIterator) throws Exception {
		VertexValueWritable vertex = getVertexValue();
		if (getSuperstep() == 1) {
			if (isPartOfSeed()){
				for (EDGETYPE et : DIR.FORWARD.edgeTypes()) {
					for (VKmer neighbor : vertex.getEdges(et)) {
						sendCheckNeighborCoverageMsg(neighbor);
					}
				}
				for (EDGETYPE et : DIR.REVERSE.edgeTypes()) {
					for (VKmer neighbor : vertex.getEdges(et)) {
						sendCheckNeighborCoverageMsg(neighbor);
					}
				}
				
			}
		}
		else if (getSuperstep() == 2){
			if (vertex.getAverageCoverage() < COVERAGE_THRESHOLD){
				while (msgIterator.hasNext()) {
					SeedRetrievalMessage msg = msgIterator.next();
		            vertex.getEdges(msg.getToPruneEdgeType().mirror()).remove(msg.getSourceVertexId());
		            SeedRetrievalMessage outgoingMsg = new SeedRetrievalMessage();
		            outgoingMsg.setSourceVertexId(getVertexId());
		            outgoingMsg.setToPruneEdgeType(msg.getToPruneEdgeType());
			}
		}
		}
		else if (getSuperstep() == 3){
			while (msgIterator.hasNext()) {
				SeedRetrievalMessage msg = msgIterator.next();
	            vertex.getEdges(msg.getToPruneEdgeType()).remove(msg.getSourceVertexId());
			}
			
		}else{
			voteToHalt();
		}
		
	}
	
	private boolean isPartOfSeed() throws Exception{
		String kmer = getVertexId().toString();
		String reversed = getVertexId().reverse().toString();
        try{
                Path pt=new Path("/home/elmira/work/seeds.txt");
                FileSystem fs = FileSystem.get(new Configuration());
                BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
                String line;
                line=br.readLine();
                while (line != null){
                	if (line.contains(kmer) || line.contains(reversed.toString())){
                		return true;
                	}
                    line=br.readLine();        
                }
        }catch(Exception e){
        }
		return false;
	}
	
	private void sendCheckNeighborCoverageMsg(VKmer neighbor){
		SeedRetrievalMessage outgoingMsg = new SeedRetrievalMessage();
		outgoingMsg.setSourceVertexId(getVertexId());
		sendMsg(neighbor, outgoingMsg);
	}
	
}
