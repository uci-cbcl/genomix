package edu.uci.ics.genomix.pregelix.operator.seeddetection;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Iterator;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configuration;

import edu.uci.ics.genomix.data.config.GenomixJobConf;
import edu.uci.ics.genomix.data.types.FileManager;
import edu.uci.ics.genomix.data.types.VKmer;
import edu.uci.ics.genomix.pregelix.base.DeBruijnGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.base.MessageWritable;
import edu.uci.ics.genomix.pregelix.base.VertexValueWritable;


public class ConfidentVertex extends DeBruijnGraphCleanVertex<VertexValueWritable, MessageWritable>{
	
	private static FileManager manager;
	private static boolean local;
	private static Path workPath;
    private static String SEED_ID;
    private Integer SEED_SCORE_THRESHOLD;
    private Integer SEED_LENGTH_THRESHOLD;
	@Override
	
	public void configure(Configuration conf) {
        super.configure(conf);
        initVertex();
        //initializeManager(conf);
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
        if (SEED_ID != null) {
            return SEED_ID.equals(getVertexId().toString());
        } else {
            if (SEED_SCORE_THRESHOLD != null) {
                return ( (getVertexValue().calculateSeedScore() >= SEED_SCORE_THRESHOLD));
            } else {
                return getVertexValue().getKmerLength() >= SEED_LENGTH_THRESHOLD;
            }
        }
	}
	/**
	public static void initializeManager(Configuration conf) {
	       try {
			manager.initialize(conf, workPath);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
   }
	
	public void writeOnFile(VKmer seed) throws IOException{
		if (workPath == null){
			workPath = manager.createDistinctFile("ConfidentSeeds", local);
		} else {
			OutputStream fos = manager.getOutputStream(workPath, local);
			ObjectOutputStream oos = new ObjectOutputStream(fos);
			oos.writeObject(seed);
			oos.close();
		}
	}
	**/
	public void saveSeed(VKmer seed) throws Exception{
        try{
                Path pt=new Path("/home/elmira/work/seeds.txt");
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
