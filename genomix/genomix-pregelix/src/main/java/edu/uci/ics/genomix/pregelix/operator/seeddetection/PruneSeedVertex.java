package edu.uci.ics.genomix.pregelix.operator.seeddetection;

import java.util.HashSet;
import java.util.Iterator;
import java.util.logging.Logger;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configuration;

import edu.uci.ics.genomix.data.config.GenomixJobConf;
import edu.uci.ics.genomix.data.types.DIR;
import edu.uci.ics.genomix.data.types.EDGETYPE;
import edu.uci.ics.genomix.data.types.VKmer;
import edu.uci.ics.genomix.pregelix.base.DeBruijnGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.base.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.operator.scaffolding2.RayVertex;

public class PruneSeedVertex extends DeBruijnGraphCleanVertex<VertexValueWritable, PruneSeedMessage>{
	
	private String workPath;
	private float SEED_COVERAGE_THRESHOLD = -1;
	private HashSet<String> seedInfo = new HashSet<>();
    public static Logger LOG = Logger.getLogger(PruneSeedVertex.class.getName()); 
    
	public void configure(Configuration conf) {
        super.configure(conf);
        initVertex();
        workPath = conf.get(GenomixJobConf.HDFS_WORK_PATH) + File.separator + String.format("CONFIDENT_SEEDS");
        if(SEED_COVERAGE_THRESHOLD == -1){
        	SEED_COVERAGE_THRESHOLD = Float.parseFloat(conf.get(GenomixJobConf.SCAFFOLDING_CONFIDENT_SEEDS_MIN_COVERAGE));
        }
        
        /**
         * Loading the confident seeds data from a file. 
         */
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
        	e.printStackTrace();
        	LOG.info("CONFIDENT_SEEDS file does not exist!");
        }
        
	}
	
	/**
	 * If I am a vertex, which belongs to a confident seed, 
	 * I need to check all of my neighbors and find those with
	 * low coverage. Then all the edges between I and these 
	 * low coverage neighbors should be pruned.
	 */
	
	@Override
	public void compute(Iterator<PruneSeedMessage> msgIterator) throws Exception {
		VertexValueWritable vertex = getVertexValue();
		if (getSuperstep() == 1) {
			if (isPartOfSeed()){
				LOG.info("vertex" + getVertexId() + "is part of a confident seed" );
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
					PruneSeedMessage msg = msgIterator.next();
		            vertex.getEdges(msg.getToPruneEdgeType().mirror()).remove(msg.getSourceVertexId());
		            LOG.info("Low Coverage vertex" + getVertexId() + "with coverage" + vertex.getAverageCoverage() +
		            		"is prunning its edge with " + msg.getSourceVertexId());
		            PruneSeedMessage outgoingMsg = new PruneSeedMessage();
		            outgoingMsg.setSourceVertexId(getVertexId());
		            outgoingMsg.setToPruneEdgeType(msg.getToPruneEdgeType());
			}
		}
		}
		else if (getSuperstep() == 3){
			while (msgIterator.hasNext()) {
				PruneSeedMessage msg = msgIterator.next();
	            vertex.getEdges(msg.getToPruneEdgeType()).remove(msg.getSourceVertexId());
	            LOG.info(" High coverage vertex" + getVertexId() + "with coverage" + vertex.getAverageCoverage() +
	            		"is prunning its edge with " + msg.getSourceVertexId());
			}
			
		}else{
			voteToHalt();
		}
		
	}
	
	private boolean isPartOfSeed() throws Exception{
		String kmer = getVertexId().toString();
		String reversed = getVertexId().reverse().toString();
        if (seedInfo.contains(kmer) || seedInfo.contains(reversed)){
        	return true;
        }
		return false;
	}
	
	
	private void sendCheckNeighborCoverageMsg(VKmer neighbor){
		PruneSeedMessage outgoingMsg = new PruneSeedMessage();
		outgoingMsg.setSourceVertexId(getVertexId());
		sendMsg(neighbor, outgoingMsg);
	}
	
}
