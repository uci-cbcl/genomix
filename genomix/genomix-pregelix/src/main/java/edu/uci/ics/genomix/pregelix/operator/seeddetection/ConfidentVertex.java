package edu.uci.ics.genomix.pregelix.operator.seeddetection;

import java.io.BufferedWriter;
import java.io.File;
import java.io.OutputStreamWriter;
import java.util.Iterator;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configuration;

import edu.uci.ics.genomix.data.config.GenomixJobConf;
import edu.uci.ics.genomix.data.types.VKmer;
import edu.uci.ics.genomix.pregelix.base.DeBruijnGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.base.MessageWritable;
import edu.uci.ics.genomix.pregelix.base.VertexValueWritable;


public class ConfidentVertex extends DeBruijnGraphCleanVertex<VertexValueWritable, MessageWritable>{
	
	private String workPath;
    private Integer SEED_SCORE_THRESHOLD;
    private Integer SEED_LENGTH_THRESHOLD;
	@Override
	
	public void configure(Configuration conf) {
        super.configure(conf);
        initVertex();
        workPath = conf.get(GenomixJobConf.HDFS_WORK_PATH) + File.separator + String.format("CONFIDENT_SEEDS");
        try {
            SEED_SCORE_THRESHOLD = Integer.parseInt(conf.get(GenomixJobConf.SCAFFOLDING_SEED_SCORE_THRESHOLD));
        } catch (NumberFormatException e) {
            SEED_LENGTH_THRESHOLD = Integer.parseInt(conf.get(GenomixJobConf.SCAFFOLDING_SEED_LENGTH_THRESHOLD));
        }
   
	}	
	
	public void compute(Iterator<MessageWritable> msgIterator) throws Exception {
		// TODO Auto-generated method stub
		if (getSuperstep() == 1) {
			if (isSeed()){
				saveSeed(getVertexValue().getInternalKmer());
			}
		}else{
			voteToHalt();
		}
		
	}
	
	private boolean isSeed(){
        if (SEED_SCORE_THRESHOLD != null) {
             return ( (getVertexValue().calculateSeedScore() >= SEED_SCORE_THRESHOLD));
        } else {
             return getVertexValue().getKmerLength() >= SEED_LENGTH_THRESHOLD;
        }
	}
	
	public void saveSeed(VKmer seed) throws Exception{
        try{
                Path pt=new Path(workPath);
                FileSystem fs = FileSystem.get(new Configuration());
                BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fs.create(pt,true)));
                fs.append(pt);
                br.write(seed.toString() + "\n");
                br.close();
        }catch(Exception e){
                System.out.println("File not found");
        }
}

}
