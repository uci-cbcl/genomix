package edu.uci.ics.genomix.pregelix.operator.seeddetection;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
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
	
	private String workPath;
	private float SEED_COVERAGE_THRESHOLD = -1;
	private ArrayList<String> seedInfo = new ArrayList<>();
	
	public void configure(Configuration conf) {
        super.configure(conf);
        initVertex();
        workPath = conf.get(GenomixJobConf.HDFS_WORK_PATH) + File.separator + String.format("CONFIDENT_SEEDS");
        if(SEED_COVERAGE_THRESHOLD == -1){
        	SEED_COVERAGE_THRESHOLD = Float.parseFloat(conf.get(GenomixJobConf.SCAFFOLDING_CONFIDENT_SEEDS_MIN_COVERAGE));
        }
        
        try{
            Path pt=new Path(workPath);
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
            String line;
            line=br.readLine();
            while (line != null){
            	for(int i = 0; i < line.length() - kmerSize +1 ; i++){
            		seedInfo.add(line.substring(i, i + kmerSize));
            	}
                line=br.readLine();        
            }
        }catch(Exception e){
        }
        
	}
	
	
	
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
			if (vertex.getAverageCoverage() < SEED_COVERAGE_THRESHOLD){
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
        if (seedInfo.contains(kmer) || seedInfo.contains(reversed.toString())){
        	return true;
        }
		return false;
	}
	
	private void sendCheckNeighborCoverageMsg(VKmer neighbor){
		SeedRetrievalMessage outgoingMsg = new SeedRetrievalMessage();
		outgoingMsg.setSourceVertexId(getVertexId());
		sendMsg(neighbor, outgoingMsg);
	}
	
}
