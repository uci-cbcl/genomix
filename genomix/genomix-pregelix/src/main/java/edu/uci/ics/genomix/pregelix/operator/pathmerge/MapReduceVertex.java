package edu.uci.ics.genomix.pregelix.operator.pathmerge;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.format.GraphCleanInputFormat;
import edu.uci.ics.genomix.pregelix.format.GraphCleanOutputFormat;
import edu.uci.ics.genomix.pregelix.io.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.type.MessageFlag;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.KmerListWritable;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.api.util.BspUtils;

public class MapReduceVertex extends
    BasicPathMergeVertex {
    
    KmerListWritable kmerList = new KmerListWritable(kmerSize);
    Map<KmerBytesWritable, KmerListWritable> kmerMapper = new HashMap<KmerBytesWritable, KmerListWritable>();
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void compute(Iterator<MessageWritable> msgIterator) {
        if(getSuperstep() == 1){
            //add a fake vertex
            Vertex vertex = (Vertex) BspUtils.createVertex(getContext().getConfiguration());
            vertex.getMsgList().clear();
            vertex.getEdges().clear();
            KmerBytesWritable vertexId = new KmerBytesWritable();
            VertexValueWritable vertexValue = new VertexValueWritable();
            vertexId.setByRead(("XXX").getBytes(), 0);
            vertexValue.setFakeVertex(true);
            vertex.setVertexValue(vertexValue);
            
            addVertex(vertexId, vertex);
        }
        else if(getSuperstep() == 2){
            if(!getVertexValue().isFakeVertex()){
                destVertexId.setByRead(("XXX").getBytes(), 0);
                outgoingMsg.setSourceVertexId(getVertexId());
                outgoingMsg.setAcutalKmer(getVertexValue().getKmer());
                sendMsg(destVertexId, outgoingMsg);
            }
            voteToHalt();
        } else if(getSuperstep() == 3){
            kmerMapper.clear();
            /** Mapper **/
            while(msgIterator.hasNext()){
                incomingMsg = msgIterator.next();
                tmpKmer.set(incomingMsg.getActualKmer());
                if(!kmerMapper.containsKey(tmpKmer)){
                    kmerList.reset();
                    kmerList.append(incomingMsg.getSourceVertexId());
                    kmerMapper.put(tmpKmer, kmerList);
                } else{
                    kmerList.set(kmerMapper.get(tmpKmer));
                    kmerMapper.put(tmpKmer, kmerList);
                }
            }
            /** Reducer **/
            for(KmerBytesWritable key : kmerMapper.keySet()){
                kmerList = kmerMapper.get(key);
                for(int i = 1; i < kmerList.getCountOfPosition(); i++){
                    //send kill message
                    outgoingMsg.setFlag(MessageFlag.KILL);
                }
            }
        } else if(getSuperstep() == 4){
            while(msgIterator.hasNext()){
                incomingMsg = msgIterator.next();
                inFlag = incomingMsg.getFlag();
                if(inFlag == MessageFlag.KILL){
                    broadcaseKillself();
                }
            }
        } else if(getSuperstep() == 5){
            responseToDeadVertex(msgIterator);
        }
    }
    
    public static void main(String[] args) throws Exception {
        PregelixJob job = new PregelixJob(MapReduceVertex.class.getSimpleName());
        job.setVertexClass(MapReduceVertex.class);
        /**
         * BinaryInput and BinaryOutput
         */
        job.setVertexInputFormatClass(GraphCleanInputFormat.class);
        job.setVertexOutputFormatClass(GraphCleanOutputFormat.class);
        job.setDynamicVertexValueSize(true);
        job.setOutputKeyClass(KmerBytesWritable.class);
        job.setOutputValueClass(VertexValueWritable.class);
        Client.run(args, job);
    }
}
