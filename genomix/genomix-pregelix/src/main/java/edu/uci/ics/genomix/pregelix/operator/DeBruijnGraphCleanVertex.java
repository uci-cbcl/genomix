package edu.uci.ics.genomix.pregelix.operator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.pregelix.format.GenericVertexToNodeOutputFormat;
import edu.uci.ics.genomix.pregelix.format.NodeToVertexInputFormat;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.common.ByteWritable;
import edu.uci.ics.genomix.pregelix.io.common.HashMapWritable;
import edu.uci.ics.genomix.pregelix.io.common.VLongWritable;
import edu.uci.ics.genomix.pregelix.io.message.MessageWritable;
import edu.uci.ics.genomix.pregelix.operator.aggregator.StatisticsAggregator;
import edu.uci.ics.genomix.type.Node;
import edu.uci.ics.genomix.type.Node.DIR;
import edu.uci.ics.genomix.type.Node.EDGETYPE;
import edu.uci.ics.genomix.type.VKmer;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.pregelix.dataflow.util.IterationUtils;

public abstract class DeBruijnGraphCleanVertex<V extends VertexValueWritable, M extends MessageWritable> extends
        Vertex<VKmer, V, NullWritable, M> {

    //logger
    public Logger LOG = Logger.getLogger(DeBruijnGraphCleanVertex.class.getName());

    public static int kmerSize = -1;
    public static int maxIteration = -1;

    public static Object lock = new Object();
    public static boolean fakeVertexExist = false;
    public static VKmer fakeVertex = new VKmer();

    public EDGETYPE[][] connectedTable = new EDGETYPE[][] { { EDGETYPE.RF, EDGETYPE.FF }, { EDGETYPE.RF, EDGETYPE.FR },
            { EDGETYPE.RR, EDGETYPE.FF }, { EDGETYPE.RR, EDGETYPE.FR } };

    protected M outgoingMsg = null;
    protected VertexValueWritable tmpValue = new VertexValueWritable();
    protected VKmer repeatKmer = null; //for detect tandemRepeat
    protected EDGETYPE repeatEdgetype; //for detect tandemRepeat
    protected short outFlag;
    protected short selfFlag;

    protected List<VKmer> problemKmers = null;
    protected boolean debug = false;
    protected boolean verbose = false;
    protected boolean logReadIds = false;

    protected HashMapWritable<ByteWritable, VLongWritable> counters = new HashMapWritable<ByteWritable, VLongWritable>();

    /**
     * initiate kmerSize, maxIteration
     */
    public void initVertex() {
        if (kmerSize == -1)
            kmerSize = Integer.parseInt(getContext().getConfiguration().get(GenomixJobConf.KMER_LENGTH));
        if (maxIteration < 0)
            maxIteration = Integer.parseInt(getContext().getConfiguration().get(
                    GenomixJobConf.GRAPH_CLEAN_MAX_ITERATIONS));
        GenomixJobConf.setGlobalStaticConstants(getContext().getConfiguration());

        checkDebug();
        //TODO fix globalAggregator
    }

    public void checkDebug() {
        debug = getContext().getConfiguration().get(GenomixJobConf.DEBUG_KMERS) != null;
        if (problemKmers == null) {
            problemKmers = new ArrayList<VKmer>();
            if (getContext().getConfiguration().get(GenomixJobConf.DEBUG_KMERS) != null) {
                for (String kmer : getContext().getConfiguration().get(GenomixJobConf.DEBUG_KMERS).split(",")) {
                    problemKmers.add(new VKmer(kmer));
                }
                Node.problemKmers = problemKmers;
            }
        }

        verbose = false;
        for (VKmer problemKmer : problemKmers) {
            verbose |= debug
                    && (getVertexValue().getNode().findEdge(problemKmer) != null || getVertexId().equals(problemKmer));
        }
    }

    /**
     * Generate random string from [ACGT]
     */
    public String generaterRandomString(int n) {
        char[] chars = "ACGT".toCharArray();
        StringBuilder sb = new StringBuilder();
        Random random = new Random();
        for (int i = 0; i < n; i++) {
            char c = chars[random.nextInt(chars.length)];
            sb.append(c);
        }
        return sb.toString();
    }

    /**
     * add fake vertex
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void addFakeVertex(String fakeKmer) {
        synchronized (lock) {
            fakeVertex.setFromStringBytes(1, fakeKmer.getBytes(), 0);
            if (!fakeVertexExist) {
                //add a fake vertex
                Vertex vertex = (Vertex) BspUtils.createVertex(getContext().getConfiguration());

                VertexValueWritable vertexValue = new VertexValueWritable();
                vertexValue.setFakeVertex(true);

                vertex.setVertexId(fakeVertex);
                vertex.setVertexValue(vertexValue);

                addVertex(fakeVertex, vertex);
                fakeVertexExist = true;
            }
        }
    }

    /**
     * increment statistics counter
     */
    public void incrementCounter(byte counterName) {
        ByteWritable counterNameWritable = new ByteWritable(counterName);
        if (counters.containsKey(counterNameWritable))
            counters.get(counterNameWritable).set(counters.get(counterNameWritable).get() + 1);
        else
            counters.put(counterNameWritable, new VLongWritable(1));
    }

    /**
     * read statistics counters
     */
    public static HashMapWritable<ByteWritable, VLongWritable> readStatisticsCounterResult(Configuration conf) {
        try {
            VertexValueWritable value = (VertexValueWritable) IterationUtils.readGlobalAggregateValue(conf,
                    BspUtils.getJobId(conf));
            return value.getCounters();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    //2013.9.21 ------------------------------------------------------------------//
    public static PregelixJob getConfiguredJob(
            GenomixJobConf conf,
            Class<? extends DeBruijnGraphCleanVertex<? extends VertexValueWritable, ? extends MessageWritable>> vertexClass)
            throws IOException {
        // the following class weirdness is because java won't let me get the runtime class in a static context :(
        PregelixJob job;
        if (conf == null)
            job = new PregelixJob(vertexClass.getSimpleName());
        else
            job = new PregelixJob(conf, vertexClass.getSimpleName());
        job.setGlobalAggregatorClass(StatisticsAggregator.class);
        job.setVertexClass(vertexClass);
        job.setVertexInputFormatClass(NodeToVertexInputFormat.class);
        job.setVertexOutputFormatClass(GenericVertexToNodeOutputFormat.class);
        job.setOutputKeyClass(VKmer.class);
        job.setOutputValueClass(VertexValueWritable.class);
        job.setDynamicVertexValueSize(true);
        return job;
    }

    /**
     * get destination vertex ex. RemoveTip
     */
    public VKmer getDestVertexId(DIR direction) {
        int degree = getVertexValue().degree(direction);
        if (degree > 1)
            throw new IllegalArgumentException(
                    "degree > 1, getDestVertexId(DIR direction) only can use for degree == 1 + \n"
                            + getVertexValue().toString());

        if (degree == 1) {
            EnumSet<EDGETYPE> edgeTypes = direction.edgeTypes();
            for (EDGETYPE et : edgeTypes) {
                if (getVertexValue().getEdgeMap(et).size() > 0)
                    return getVertexValue().getEdgeMap(et).firstKey();
            }
        }
        //degree in this direction == 0
        throw new IllegalArgumentException(
                "degree > 0, getDestVertexId(DIR direction) only can use for degree == 1 + \n"
                        + getVertexValue().toString());
    }

    /**
     * check if I am a tandemRepeat
     */
    public boolean isTandemRepeat(VertexValueWritable value) {
        for (EDGETYPE et : EDGETYPE.values()) {
            for (VKmer kmerToCheck : value.getEdgeMap(et).keySet()) {
                if (kmerToCheck.equals(getVertexId())) {
                    repeatEdgetype = et;
                    repeatKmer.setAsCopy(kmerToCheck);
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * broadcastKillself ex. RemoveLow, RemoveTip, RemoveBridge
     */
    public void broadcastKillself() {
        VertexValueWritable vertex = getVertexValue();
        for (EDGETYPE et : EDGETYPE.values()) {
            for (VKmer dest : vertex.getEdgeMap(et).keySet()) {
                outgoingMsg.reset();
                outFlag &= EDGETYPE.CLEAR;
                outFlag |= et.mirror().get();
                outgoingMsg.setFlag(outFlag);
                outgoingMsg.setSourceVertexId(getVertexId());
                sendMsg(dest, outgoingMsg);
                if (verbose) {
                    LOG.fine("Iteration " + getSuperstep() + "\r\n" + "Vertex Id: " + getVertexId() + "\r\n"
                            + "Vertex Value: " + getVertexValue() + "\r\n\n"); // TODO killSelf in log
                }
            }
        }
    }

    /**
     * Ex. A and B are bubbles and we want to keep A and delete B.
     * B will receive kill msg from majorVertex and then broadcast killself to all the neighbor to delete the edge which points to B.
     * Here, pruneDeadEdges() is when one vertex receives msg from B,
     * it needs to delete the edge which points to B
     * //TODO use general remove and process update function
     */
    public void pruneDeadEdges(Iterator<M> msgIterator) {
        if (verbose) {
            LOG.fine("Before update " + "\r\n" + "My vertexId is " + getVertexId() + "\r\n" + "My vertexValue is "
                    + getVertexValue() + "\r\n\n");
        }
        MessageWritable incomingMsg;
        while (msgIterator.hasNext()) {
            incomingMsg = msgIterator.next();
            EDGETYPE meToNeighborEdgetype = EDGETYPE.fromByte(incomingMsg.getFlag());
            getVertexValue().getEdgeMap(meToNeighborEdgetype).remove(incomingMsg.getSourceVertexId());

            if (verbose) {
                LOG.fine("Receive message from dead node!" + incomingMsg.getSourceVertexId() + "\r\n"
                        + "The deadToMeEdgetype in message is: " + meToNeighborEdgetype + "\r\n\n");
            }
        }
        if (verbose) {
            LOG.fine("After update " + "\r\n" + "My vertexId is " + getVertexId() + "\r\n" + "My vertexValue is "
                    + getVertexValue() + "\r\n\n");
        }
    }

    /**
     * send message to all neighbor nodes
     */
    public void sendSettledMsgs(DIR direction, VertexValueWritable value) {
        VertexValueWritable vertex = getVertexValue();
        for (EDGETYPE et : direction.edgeTypes()) {
            for (VKmer dest : vertex.getEdgeMap(et).keySet()) {
                //                outgoingMsg.reset();
                outFlag &= EDGETYPE.CLEAR;
                outFlag |= et.mirror().get();
                outgoingMsg.setFlag(outFlag);
                outgoingMsg.setSourceVertexId(getVertexId());
                sendMsg(dest, outgoingMsg);
            }
        }
    }

    public void sendSettledMsgToAllNeighborNodes(VertexValueWritable value) {
        sendSettledMsgs(DIR.REVERSE, value);
        sendSettledMsgs(DIR.FORWARD, value);
    }
}