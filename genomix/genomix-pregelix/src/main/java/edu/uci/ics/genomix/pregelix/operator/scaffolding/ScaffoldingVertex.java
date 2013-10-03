package edu.uci.ics.genomix.pregelix.operator.scaffolding;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;

import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.common.ArrayListWritable;
import edu.uci.ics.genomix.pregelix.io.common.HashMapWritable;
import edu.uci.ics.genomix.pregelix.io.message.BFSTraverseMessage;
import edu.uci.ics.genomix.pregelix.io.message.MessageWritable;
import edu.uci.ics.genomix.pregelix.operator.BasicGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.operator.aggregator.StatisticsAggregator;
import edu.uci.ics.genomix.pregelix.type.EdgeType;
import edu.uci.ics.genomix.pregelix.type.StatisticsCounter;
import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.type.Node.DIR;
import edu.uci.ics.genomix.type.Node.EDGETYPE;
import edu.uci.ics.genomix.type.ReadHeadSet;
import edu.uci.ics.genomix.type.ReadHeadInfo;
import edu.uci.ics.genomix.type.VKmer;
import edu.uci.ics.genomix.type.VKmerList;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.pregelix.dataflow.util.IterationUtils;

/**
 * Graph clean pattern: Scaffolding
 * 
 * @author anbangx
 */
public class ScaffoldingVertex extends BFSTraverseVertex {

    // TODO Optimization: send map to readId.hashValue() bin
    // TODO BFS can seperate into simple BFS to filter and real BFS
    
    // add to driver
    public static int SCAFFOLDING_MIN_TRAVERSAL_LENGTH = 20;
    public static int SCAFFOLDING_MAX_TRAVERSAL_LENGTH = 100;
    public static int SCAFFOLDING_MIN_COVERAGE = 20;
    public static int SCAFFOLDING_MIN_LENGTH = 20;
    
    public static int NUM_STEP_END_BFS = SCAFFOLDING_MAX_TRAVERSAL_LENGTH - kmerSize + 3;

    private HashMapWritable<LongWritable, ArrayListWritable<SearchInfo>> scaffoldingMap = new HashMapWritable<LongWritable, ArrayListWritable<SearchInfo>>();

    @Override
    public void initVertex() {
        super.initVertex();
        if (getSuperstep() == 1)
            StatisticsAggregator.preGlobalCounters.clear();
        else if(getSuperstep() == 2)
            StatisticsAggregator.preGlobalCounters = BasicGraphCleanVertex.readStatisticsCounterResult(getContext().getConfiguration());
        if (getSuperstep() == 1)
            ScaffoldingAggregator.preScaffoldingMap.clear();
        else if (getSuperstep() == 2)
            ScaffoldingAggregator.preScaffoldingMap = readScaffoldingMapResult(getContext().getConfiguration());
        counters.clear();
        scaffoldingMap.clear();
        getVertexValue().getCounters().clear();
        getVertexValue().getScaffoldingMap().clear();
    }

    public void addReadsToScaffoldingMap(ReadHeadSet readIds, READHEAD_TYPE isFlip) {
        // searchInfo can be a struct but not a class
        SearchInfo searchInfo;
        ArrayListWritable<SearchInfo> searchInfoList;

        for (ReadHeadInfo pos : readIds) {
            long readId = pos.getReadId();
            if (scaffoldingMap.containsKey(readId)) {
                searchInfoList = scaffoldingMap.get(readId);
            } else {
                searchInfoList = new ArrayListWritable<SearchInfo>();
                scaffoldingMap.put(new LongWritable(readId), searchInfoList);
            }
            searchInfo = new SearchInfo(getVertexId(), isFlip);
            searchInfoList.add(searchInfo);
        }
    }
    

    public int updateBFSLength(BFSTraverseMessage incomingMsg, UPDATELENGTH_TYPE type){
        VertexValueWritable vertex = getVertexValue();
        ReadHeadSet readHeadSet = incomingMsg.isDestFlip() ? vertex.getEndReads() : vertex.getStartReads();
        switch(type){
            // remember to account for partial overlaps
            case OFFSET:
                //FIXME is it offset or (length - offset)
                return incomingMsg.getTotalBFSLength() + readHeadSet.getOffsetFromReadId(incomingMsg.getReadId()) - kmerSize + 1; 
            case WHOLE_LENGTH:
                return incomingMsg.getTotalBFSLength() + vertex.getInternalKmer().getLength() - kmerSize + 1;
            default:
                throw new IllegalStateException("Update length type only has two kinds: offset and whole_length!");
        }
        
    }
    
    public void sendMsgAndUpdateEdgeTypeList(ArrayListWritable<EdgeType> edgeTypeList, DIR direction){
        VertexValueWritable vertex = getVertexValue();
        for (EDGETYPE et : direction.edgeTypes()) {
            for (VKmer dest : vertex.getEdgeList(et).keySet()) {
                outFlag &= EDGETYPE.CLEAR;
                outFlag |= et.mirror().get();
                outgoingMsg.setFlag(outFlag);
                // update EdgeTypeList
                edgeTypeList.add(new EdgeType(et));
                outgoingMsg.setEdgeTypeList(edgeTypeList);
                sendMsg(dest, outgoingMsg);
            }
        }
    }
    
    public boolean isInRange(int traversalLength) {
        return traversalLength < SCAFFOLDING_MAX_TRAVERSAL_LENGTH && traversalLength
                > SCAFFOLDING_MIN_TRAVERSAL_LENGTH;
    }
    
    /**
     * step 1:
     */
    public void generateScaffoldingMap() {
        // add a fake vertex 
        addFakeVertex("A");
        // grouped by 5'/~5' readId in aggregator
        VertexValueWritable vertex = getVertexValue();
        if (vertex.getAverageCoverage() >= SCAFFOLDING_MIN_COVERAGE
                && vertex.getInternalKmer().getLength() < SCAFFOLDING_MIN_LENGTH) {
            addReadsToScaffoldingMap(vertex.getStartReads(), READHEAD_TYPE.UNFLIPPED);
            addReadsToScaffoldingMap(vertex.getEndReads(), READHEAD_TYPE.FLIPPED);
            vertex.setScaffoldingMap(scaffoldingMap);
        }
        voteToHalt();
    }

    /**
     * step 2:
     */
    public void processScaffoldingMap() {
        // fake vertex process scaffoldingMap 
        ArrayListWritable<SearchInfo> searchInfoList;
        for (Entry<LongWritable, ArrayListWritable<SearchInfo>> entry : ScaffoldingAggregator.preScaffoldingMap
                .entrySet()) {
            searchInfoList = entry.getValue();
            if (searchInfoList.size() > 2)
                throw new IllegalStateException(
                        "The size of SearchInfoList should be not bigger than 2, but here its size " + "is "
                                + searchInfoList.size() + "!");
            if (searchInfoList.size() == 2) {
                outgoingMsg.reset();
                VKmer srcNode = setOutgoingSrcAndDest(entry.getKey().get(), searchInfoList);
                sendMsg(srcNode, outgoingMsg);
            }
        }

        deleteVertex(getVertexId());
    }
    
    /**
     * step 3:
     */
    public void BFSearch(Iterator<BFSTraverseMessage> msgIterator) {
        VertexValueWritable vertex = getVertexValue();
        BFSTraverseMessage incomingMsg;
        while (msgIterator.hasNext()) {
            incomingMsg = msgIterator.next();
            // For dest node -- save PathList and EdgeTypeList if valid (stop when ambiguous)
            int totalBFSLength;
            if(incomingMsg.getSeekedVertexId().equals(getVertexId()) && isValidDestination(incomingMsg)){
                // update totalBFSLength 
                totalBFSLength = updateBFSLength(incomingMsg, UPDATELENGTH_TYPE.OFFSET);
                long commonReadId = incomingMsg.getReadId();
                if(isInRange(totalBFSLength)){ 
                    HashMapWritable<LongWritable, PathAndEdgeTypeList> pathMap = vertex.getPathMap();
                    if(pathMap.containsKey(commonReadId)){ // if it's ambiguous path
                        // put empty in value to mark it as ambiguous path
                        pathMap.get(commonReadId).reset();
                        vertex.setSaved(false);
                        continue;
                    } else{ // if it's unambiguous path, save 
                        VKmerList updatedKmerList = new VKmerList(incomingMsg.getPathList());
                        updatedKmerList.append(getVertexId());
                        // doesn't need to update edgeTypeList
                        PathAndEdgeTypeList pathAndEdgeTypeList = new PathAndEdgeTypeList(updatedKmerList, incomingMsg.getEdgeTypeList());
                        pathMap.put(new LongWritable(commonReadId), pathAndEdgeTypeList);
                        vertex.setSaved(true);
                    }
                }  
            }
            // For all nodes -- send messge to all neighbor if there exists valid path
            totalBFSLength = updateBFSLength(incomingMsg, UPDATELENGTH_TYPE.WHOLE_LENGTH);
            if(totalBFSLength <= SCAFFOLDING_MAX_TRAVERSAL_LENGTH){
                // setup ougoingMsg and prepare to sendMsg
                outgoingMsg.reset();
                
                // update totalBFSLength 
                outgoingMsg.setTotalBFSLength(totalBFSLength);
                
                // update PathList
                VKmerList updatedKmerList = incomingMsg.getPathList();
                updatedKmerList.append(getVertexId());
                outgoingMsg.setPathList(updatedKmerList);
                
                // send message to valid neighbor
                ArrayListWritable<EdgeType> oldEdgeTypeList = incomingMsg.getEdgeTypeList();
                if(getSuperstep() == 3){ // the initial BFS
                    // send message to all neighbors and update EdgeTypeList
                    sendMsgAndUpdateEdgeTypeList(oldEdgeTypeList, DIR.REVERSE);
                    sendMsgAndUpdateEdgeTypeList(oldEdgeTypeList, DIR.FORWARD);
                } else{
                    // A -> B -> C, neighor: A, me: B, validDir: B -> C 
                    EDGETYPE meToPrev = EDGETYPE.fromByte(incomingMsg.getFlag());
                    DIR validMeToNextDir = meToPrev.dir().mirror();
                    
                    // send message to valid neighbors and update EdgeTypeList
                    sendMsgAndUpdateEdgeTypeList(oldEdgeTypeList, validMeToNextDir);
                }
            }
        }
        if(vertex.isSaved())
            activate();
        else
            voteToHalt();
    }
    
    /**
     * send message to all the path nodes to add this common readId
     */
    public void sendMsgToPathNodeToAddCommondReadId(HashMapWritable<LongWritable, PathAndEdgeTypeList> pathMap) {
        VertexValueWritable vertex = getVertexValue();
        for(LongWritable commonReadId : pathMap.keySet()){
            outgoingMsg.reset();
            outgoingMsg.setReadId(commonReadId.get());
            
            PathAndEdgeTypeList pathAndEdgeTypeList = pathMap.get(commonReadId);
            VKmerList kmerList = pathAndEdgeTypeList.getKmerList();
            ArrayListWritable<EdgeType> edgeTypeList = pathAndEdgeTypeList.getEdgeTypeList();
            // in outgoingMsg.flag (0-1)bit for nextEdgeType, (2-3)bit for prevEdgeType
            for(int i = 0; i < pathAndEdgeTypeList.size(); i++){
                if(i == pathAndEdgeTypeList.size() - 1){ // only update self
                    EDGETYPE prevToMe = edgeTypeList.get(i - 1).getEdgeType();
                    VKmer preKmer = kmerList.getPosition(i - 1);
                    vertex.getEdgeList(prevToMe.mirror()).get(preKmer).add(commonReadId.get());
                } else{
                    VKmerList pathList = outgoingMsg.getPathList();
                    // next (0-1)bit
                    outFlag &= EDGETYPE.CLEAR;
                    outFlag |= edgeTypeList.get(i).getEdgeTypeByte();
                    VKmer nextKmer = kmerList.getPosition(i + 1);
                    pathList.append(nextKmer); // msg.pathList[0] stores nextKmer
                    
                    if(i > 0){
                        // prev (2-3)bit
                        outFlag &= EDGETYPE.CLEAR << 2;
                        outFlag |= edgeTypeList.get(i - 1).getMirrorEdgeTypeByte() << 2;
                        VKmer preKmer = kmerList.getPosition(i - 1);
                        pathList.append(preKmer); // msg.pathList[1] stores preKmer 
                        
                        outgoingMsg.setFlag(outFlag);
                        sendMsg(kmerList.getPosition(i), outgoingMsg);
                    }
                }
            }
        }
    }
    
    /**
     * step 4:
     */
    public void sendMsgToPathNode(){
        VertexValueWritable vertex = getVertexValue();
        // send message to all the path nodes to add this common readId
        sendMsgToPathNodeToAddCommondReadId(vertex.getPathMap());
        //set statistics counter: Num_Scaffodings
        incrementCounter(StatisticsCounter.Num_Scaffodings);
        vertex.setCounters(counters);
    }
    
    /**
     * append common readId to the corresponding edge
     */
    public void appendCommonReadId(Iterator<BFSTraverseMessage> msgIterator) {
        VertexValueWritable vertex = getVertexValue();
        while(msgIterator.hasNext()){
            BFSTraverseMessage incomingMsg = msgIterator.next();
            long commonReadId = incomingMsg.getReadId();
            VKmerList pathList = incomingMsg.getPathList();
            if(pathList.size() > 2)
                throw new IllegalStateException("When path node receives message to append common readId," +
                        "PathList should only have one(next) or two(prev and next) elements");
            VKmer nextKmer = incomingMsg.getPathList().getPosition(0);
            EDGETYPE meToNext = EDGETYPE.fromByte(incomingMsg.getFlag());
            vertex.getEdgeList(meToNext).get(nextKmer).add(commonReadId);
            
            if(incomingMsg.getPathList().size() == 2){
                VKmer prevKmer = incomingMsg.getPathList().getPosition(1);
                EDGETYPE meToPrev = EDGETYPE.fromByte((short)(incomingMsg.getFlag() << 2));
                vertex.getEdgeList(meToPrev).get(prevKmer).add(commonReadId);
            }
        }
    }
    
    @Override
    public void compute(Iterator<BFSTraverseMessage> msgIterator) {
        initVertex();
        if (getSuperstep() == 1) {
            generateScaffoldingMap();
        } else if (getSuperstep() == 2) {
            processScaffoldingMap();
        } else if (getSuperstep() >= 3) {
            BFSearch(msgIterator);
        } else if (getSuperstep() > NUM_STEP_END_BFS){
            sendMsgToPathNode();
            voteToHalt();
        } else if (getSuperstep() == NUM_STEP_END_BFS + 1){
            appendCommonReadId(msgIterator);
        }
    }

    public static HashMapWritable<LongWritable, ArrayListWritable<SearchInfo>> readScaffoldingMapResult(
            Configuration conf) {
        try {
            VertexValueWritable value = (VertexValueWritable) IterationUtils.readGlobalAggregateValue(conf,
                    BspUtils.getJobId(conf));
            return value.getScaffoldingMap();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public static void main(String[] args) throws Exception {
        Client.run(args, getConfiguredJob(null, ScaffoldingVertex.class));
    }

    public static PregelixJob getConfiguredJob(GenomixJobConf conf,
            Class<? extends BasicGraphCleanVertex<? extends VertexValueWritable, ? extends MessageWritable>> vertexClass)
            throws IOException {
        PregelixJob job = BasicGraphCleanVertex.getConfiguredJob(conf, vertexClass);
        job.setGlobalAggregatorClass(ScaffoldingAggregator.class);
        return job;
    }
}
