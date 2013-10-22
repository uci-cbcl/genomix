package edu.uci.ics.genomix.pregelix.operator.splitrepeat;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.AbstractMap.SimpleEntry;

import org.apache.hadoop.io.NullWritable;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.message.SplitRepeatMessage;
import edu.uci.ics.genomix.pregelix.operator.DeBruijnGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.operator.aggregator.StatisticsAggregator;
import edu.uci.ics.genomix.pregelix.type.StatisticsCounter;
import edu.uci.ics.genomix.type.EDGETYPE;
import edu.uci.ics.genomix.type.EdgeMap;
import edu.uci.ics.genomix.type.Node.NeighborInfo;
import edu.uci.ics.genomix.type.ReadIdSet;
import edu.uci.ics.genomix.type.VKmer;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.util.BspUtils;

/**
 * Graph clean pattern: Split Repeat
 * Details: This component identifies small repeats that are spanned by sets of
 * reads. The algorithms are similar to scaffolding, but uses individual
 * reads. It is very experimental, with marginal improvements to the graph
 * ex. a -r1-> b -r1-> c
 *     d -r2->   -r2-> e 
 *     after Split Repeat, you can get
 *     a -r1-> b' -r1-> c
 *     d -r2-> b'' -r2-> e         
 */
public class SplitRepeatVertex extends DeBruijnGraphCleanVertex<VertexValueWritable, SplitRepeatMessage> {

    public static final int NUM_LETTERS_TO_APPEND = 3;
    private static long randSeed = 1; //static for save memory
    private Random randGenerator = null;

    private HashSet<String> existKmerString = new HashSet<String>();

    /**
     * initiate kmerSize, maxIteration
     */
    @Override
    public void initVertex() {
        super.initVertex();
        if (outgoingMsg == null)
            outgoingMsg = new SplitRepeatMessage();
        randSeed = Long.parseLong(getContext().getConfiguration().get(GenomixJobConf.RANDOM_RANDSEED)); // also can use getSuperstep(), because it is better to debug under deterministically random
        if (randGenerator == null)
            randGenerator = new Random(randSeed);
        StatisticsAggregator.preGlobalCounters.clear();
        //        else
        //            StatisticsAggregator.preGlobalCounters = BasicGraphCleanVertex.readStatisticsCounterResult(getContext().getConfiguration());
        counters.clear();
        getVertexValue().getCounters().clear();
    }

    /**
     * Generate random string from [ACGT]
     */
    public String generaterRandomDNAString(int n, String vertexId) {
        char[] chars = "ACGT".toCharArray();
        StringBuilder sb = new StringBuilder();
        // The maximum edge number of one vertex is 8, so 4**num_letters is always bigger than len(existing)
        while (true) { // impossible infinite loop
            for (int i = 0; i < n; i++) {
                char c = chars[randGenerator.nextInt(chars.length)];
                sb.append(c);
            }
            if (!existKmerString.contains(vertexId + sb.toString()))
                break;
        }
        existKmerString.add(vertexId + sb.toString());
        return sb.toString();
    }

    public VKmer randomGenerateVertexId(int numOfSuffix) {
        String newVertexId = getVertexId().toString() + generaterRandomDNAString(numOfSuffix);
        VKmer createdVertexId = new VKmer();
        createdVertexId.setFromStringBytes(kmerSize + numOfSuffix, newVertexId.getBytes(), 0);
        return createdVertexId;
    }

    public void createNewVertex(VKmer createdVertexId, NeighborInfo reverseNeighborInfo,
            NeighborInfo forwardNeighborInfo) {
        Vertex<VKmer, VertexValueWritable, NullWritable, SplitRepeatMessage> newVertex = BspUtils
                .createVertex(getContext().getConfiguration());
        VertexValueWritable vertexValue = new VertexValueWritable();
        //add the corresponding edge to new vertex
        vertexValue.getEdgeMap(reverseNeighborInfo.et).put(reverseNeighborInfo.kmer,
                new ReadIdSet(reverseNeighborInfo.readIds));
        vertexValue.getEdgeMap(forwardNeighborInfo.et).put(forwardNeighborInfo.kmer,
                new ReadIdSet(forwardNeighborInfo.readIds));

        vertexValue.setInternalKmer(getVertexId());

        newVertex.setVertexId(createdVertexId);
        newVertex.setVertexValue(vertexValue);

        addVertex(createdVertexId, newVertex);
    }

    public void updateNeighbors(VKmer createdVertexId, ReadIdSet edgeIntersection, NeighborInfo newReverseNeighborInfo,
            NeighborInfo newForwardNeighborInfo) {
        outgoingMsg.reset();
        outgoingMsg.setSourceVertexId(getVertexId());
        outgoingMsg.setCreatedEdge(createdVertexId, edgeIntersection);

        EDGETYPE neighborToRepeat = newReverseNeighborInfo.et.mirror();
        outgoingMsg.setFlag(neighborToRepeat.get());
        sendMsg(newReverseNeighborInfo.kmer, outgoingMsg);

        neighborToRepeat = newForwardNeighborInfo.et.mirror();
        outgoingMsg.setFlag(neighborToRepeat.get());
        sendMsg(newForwardNeighborInfo.kmer, outgoingMsg);
    }

    public void deleteEdgeFromOldVertex(Set<NeighborInfo> neighborsInfo) {
        for (NeighborInfo neighborInfo : neighborsInfo)
            getVertexValue().getEdgeMap(neighborInfo.et).removeReadIdSubset(neighborInfo.kmer, neighborInfo.readIds);
    }

    public void detectRepeatAndSplit() {
        if (getVertexValue().getDegree() > 2) { // if I may be a repeat which can be split
            Set<NeighborInfo> deletedNeighborsInfo = new HashSet<NeighborInfo>();
            VertexValueWritable vertex = getVertexValue();
            // process validPathsTable
            // validPathsTable: a table representing the set of edge types forming a valid path from
            //                 A--et1-->B--et2-->C with et1 being the first dimension and et2 being 
            //                 the second
            for (int i = 0; i < validPathsTable.length; i++) {
                // set edgeType and the corresponding edgeList based on connectedTable
                EDGETYPE reverseEdgeType = validPathsTable[i][0];
                EDGETYPE forwardEdgeType = validPathsTable[i][1];
                EdgeMap reverseEdgeList = vertex.getEdgeMap(reverseEdgeType);
                EdgeMap forwardEdgeList = vertex.getEdgeMap(forwardEdgeType);

                for (Entry<VKmer, ReadIdSet> reverseEdge : reverseEdgeList.entrySet()) {
                    for (Entry<VKmer, ReadIdSet> forwardEdge : forwardEdgeList.entrySet()) {
                        // set neighborEdge readId intersection
                        ReadIdSet edgeIntersection = reverseEdge.getValue().getIntersection(forwardEdge.getValue());

                        if (!edgeIntersection.isEmpty()) {
                            // random generate vertexId of new vertex // TODO create new vertex when add letters, the #letter depends on the time, which can't cause collision
                            VKmer createdVertexId = randomGenerateVertexId(NUM_LETTERS_TO_APPEND);

                            // change new incomingEdge/outgoingEdge's edgeList to commondReadIdSet
                            NeighborInfo newReverseNeighborInfo = new NeighborInfo(reverseEdgeType,
                                    reverseEdge.getKey(), edgeIntersection);
                            NeighborInfo newForwardNeighborInfo = new NeighborInfo(forwardEdgeType,
                                    forwardEdge.getKey(), edgeIntersection);

                            // create new/created vertex which has new incomingEdge/outgoingEdge
                            createNewVertex(createdVertexId, newReverseNeighborInfo, newForwardNeighborInfo);

                            //set statistics counter: Num_SplitRepeats
                            incrementCounter(StatisticsCounter.Num_SplitRepeats);
                            vertex.setCounters(counters);

                            // send msg to neighbors to update their edges to new vertex 
                            updateNeighbors(createdVertexId, edgeIntersection, newReverseNeighborInfo,
                                    newForwardNeighborInfo);

                            // store deleteSet
                            deletedNeighborsInfo.add(newReverseNeighborInfo);
                            deletedNeighborsInfo.add(newForwardNeighborInfo);
                        }
                    }
                }
            }

            // process deletedNeighborInfo -- delete extra edges from old vertex
            deleteEdgeFromOldVertex(deletedNeighborsInfo);

            // Old vertex delete or voteToHalt 
            if (getVertexValue().getDegree() == 0)//if no any edge, delete
                deleteVertex(getVertexId());
            else
                voteToHalt();
        }
    }

    public void responseToRepeat(Iterator<SplitRepeatMessage> msgIterator) {
        while (msgIterator.hasNext()) {
            SplitRepeatMessage incomingMsg = msgIterator.next();

            // update edgelist to new/created vertex
            EDGETYPE meToNeighbor = EDGETYPE.fromByte(incomingMsg.getFlag());
            Entry<VKmer, ReadIdSet> createdEdge = incomingMsg.getCreatedEdge();
            Entry<VKmer, ReadIdSet> deletedEdge = new SimpleEntry<VKmer, ReadIdSet>(incomingMsg.getSourceVertexId(),
                    createdEdge.getValue());

            getVertexValue().getEdgeMap(meToNeighbor).put(createdEdge.getKey(), new ReadIdSet(createdEdge.getValue()));
            getVertexValue().getEdgeMap(meToNeighbor).removeReadIdSubset(deletedEdge.getKey(), deletedEdge.getValue());
        }
    }

    @Override
    public void compute(Iterator<SplitRepeatMessage> msgIterator) {
        if (getSuperstep() == 1) {
            initVertex();
            detectRepeatAndSplit();
        } else if (getSuperstep() == 2) {
            responseToRepeat(msgIterator);
            voteToHalt();
        }
    }

    public static void main(String[] args) throws Exception {
        Client.run(args, getConfiguredJob(null, SplitRepeatVertex.class));
    }

    //TODO split repeat should move start/end readids that are present in the intersection readids to the new node
}
