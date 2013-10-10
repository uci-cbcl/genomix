package edu.uci.ics.genomix.pregelix.operator.scaffolding;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.io.ScaffoldingVertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.common.ArrayListWritable;
import edu.uci.ics.genomix.pregelix.io.common.HashMapWritable;
import edu.uci.ics.genomix.pregelix.io.message.BFSTraverseMessage;
import edu.uci.ics.genomix.pregelix.io.message.MessageWritable;
import edu.uci.ics.genomix.pregelix.operator.DeBruijnGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.operator.aggregator.StatisticsAggregator;
import edu.uci.ics.genomix.type.ReadHeadInfo;
import edu.uci.ics.genomix.type.ReadHeadSet;
import edu.uci.ics.genomix.type.VKmer;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.pregelix.dataflow.util.IterationUtils;

/**
 * Graph clean pattern: Scaffolding
 */
public class ScaffoldingVertex extends BFSTraverseVertex {

    // TODO Optimization: send map to readId.hashValue() bin
    // TODO BFS can seperate into simple BFS to filter and real BFS

    public static int NUM_STEP_END_BFS = -1;

    private HashMapWritable<LongWritable, ArrayListWritable<SearchInfo>> scaffoldingMap = new HashMapWritable<LongWritable, ArrayListWritable<SearchInfo>>();

    @Override
    public void initVertex() {
        super.initVertex();
        if (SCAFFOLDING_MIN_TRAVERSAL_LENGTH < 0)
            SCAFFOLDING_MIN_TRAVERSAL_LENGTH = Integer.parseInt(getContext().getConfiguration().get(
                    GenomixJobConf.SCAFFOLDING_MIN_TRAVERSAL_LENGTH));
        if (SCAFFOLDING_MAX_TRAVERSAL_LENGTH < 0)
            SCAFFOLDING_MAX_TRAVERSAL_LENGTH = Integer.parseInt(getContext().getConfiguration().get(
                    GenomixJobConf.SCAFFOLDING_MAX_TRAVERSAL_LENGTH));
        if (SCAFFOLDING_VERTEX_MIN_COVERAGE < 0)
            SCAFFOLDING_VERTEX_MIN_COVERAGE = Integer.parseInt(getContext().getConfiguration().get(
                    GenomixJobConf.SCAFFOLDING_VERTEX_MIN_COVERAGE));
        if (SCAFFOLDING_VERTEX_MIN_LENGTH < 0)
            SCAFFOLDING_VERTEX_MIN_LENGTH = Integer.parseInt(getContext().getConfiguration().get(
                    GenomixJobConf.SCAFFOLDING_VERTEX_MIN_LENGTH));
        if (NUM_STEP_END_BFS < 0)
            NUM_STEP_END_BFS = SCAFFOLDING_MAX_TRAVERSAL_LENGTH - kmerSize + 3;
        if (getSuperstep() == 1)
            StatisticsAggregator.preGlobalCounters.clear();
        else if (getSuperstep() == 2)
            StatisticsAggregator.preGlobalCounters = DeBruijnGraphCleanVertex.readStatisticsCounterResult(getContext()
                    .getConfiguration());
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

    /**
     * step 1:
     */
    public void generateScaffoldingMap() {
        // add a fake vertex 
        addFakeVertex("A");
        // grouped by 5'/~5' readId in aggregator
        ScaffoldingVertexValueWritable vertex = getVertexValue();
        if (vertex.isValidScaffoldingSearchNode()) {
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
                VKmer srcNode = setOutgoingMsgSrcAndDest(entry.getKey().get(), searchInfoList);
                sendMsg(srcNode, outgoingMsg);
            }
        }

        deleteVertex(getVertexId());
    }

    @Override
    public void compute(Iterator<BFSTraverseMessage> msgIterator) {
        initVertex();
        if (getSuperstep() == 1) {
            generateScaffoldingMap();
        } else if (getSuperstep() == 2) {
            processScaffoldingMap();
        } else if (getSuperstep() >= 3) {
            if (getSuperstep() == 3)
                BFSearch(msgIterator, SEARCH_TYPE.BEGIN_SEARCH);
            else
                BFSearch(msgIterator, SEARCH_TYPE.CONTINUE_SEARCH);
        } else if (getSuperstep() == NUM_STEP_END_BFS) {
            sendMsgToPathNode();
            voteToHalt();
        } else if (getSuperstep() == NUM_STEP_END_BFS + 1) {
            appendCommonReadId(msgIterator);
            voteToHalt();
        } else {
            throw new IllegalStateException("Programmer error!!!");
        }
    }

    public static HashMapWritable<LongWritable, ArrayListWritable<SearchInfo>> readScaffoldingMapResult(
            Configuration conf) {
        try {
            ScaffoldingVertexValueWritable value = (ScaffoldingVertexValueWritable) IterationUtils
                    .readGlobalAggregateValue(conf, BspUtils.getJobId(conf));
            return value.getScaffoldingMap();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public static void main(String[] args) throws Exception {
        Client.run(args, getConfiguredJob(null, ScaffoldingVertex.class));
    }

    public static PregelixJob getConfiguredJob(
            GenomixJobConf conf,
            Class<? extends DeBruijnGraphCleanVertex<? extends VertexValueWritable, ? extends MessageWritable>> vertexClass)
            throws IOException {
        PregelixJob job = DeBruijnGraphCleanVertex.getConfiguredJob(conf, vertexClass);
        job.setGlobalAggregatorClass(ScaffoldingAggregator.class);
        return job;
    }
}
