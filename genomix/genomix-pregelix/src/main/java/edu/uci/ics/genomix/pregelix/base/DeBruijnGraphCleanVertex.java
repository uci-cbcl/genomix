package edu.uci.ics.genomix.pregelix.base;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;
import java.util.logging.Logger;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counters;

import edu.uci.ics.genomix.data.config.GenomixJobConf;
import edu.uci.ics.genomix.data.types.DIR;
import edu.uci.ics.genomix.data.types.EDGETYPE;
import edu.uci.ics.genomix.data.types.Node;
import edu.uci.ics.genomix.data.types.VKmer;
import edu.uci.ics.genomix.pregelix.aggregator.DeBruijnVertexCounterAggregator;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.pregelix.api.util.HadoopCountersAggregator.ResettableCounters;

public abstract class DeBruijnGraphCleanVertex<V extends VertexValueWritable, M extends MessageWritable> extends
        Vertex<VKmer, V, NullWritable, M> {

    //logger
    public Logger LOG = Logger.getLogger(DeBruijnGraphCleanVertex.class.getName());

    public enum MESSAGETYPE {

        UPDATE((byte) (1 << 4)),
        TO_NEIGHBOR((byte) (2 << 4)),
        REPLACE_NODE((byte) (3 << 4)),
        KILL_SELF((byte) (4 << 4)),
        FROM_DEAD((byte) (5 << 4)),
        ADD_READIDS((byte) (6 << 4));

        public static final byte MASK = (byte) (0b111 << 4);
        public static final byte CLEAR = (byte) (0b0001111);

        private final byte val;

        private MESSAGETYPE(byte val) {
            this.val = val;
        }

        public final byte get() {
            return val;
        }

        public static MESSAGETYPE fromByte(short b) {
            b &= MASK;
            if (b == UPDATE.val)
                return UPDATE;
            if (b == TO_NEIGHBOR.val)
                return TO_NEIGHBOR;
            if (b == REPLACE_NODE.val)
                return REPLACE_NODE;
            if (b == KILL_SELF.val)
                return KILL_SELF;
            if (b == FROM_DEAD.val)
                return FROM_DEAD;
            if (b == ADD_READIDS.val)
                return ADD_READIDS;
            return null;

        }
    }

    public static int kmerSize = -1;
    public static int maxIteration = -1;

    public static Object lock = new Object();
    public static boolean fakeVertexExist = false;
    public static VKmer fakeVertex = new VKmer();

    // validPathsTable: a table representing the set of edge types forming a valid path from
    //                 A--et1-->B--et2-->C with et1 being the first dimension and et2 being 
    //                 the second
    public static final EDGETYPE[][] validPathsTable = new EDGETYPE[][] { { EDGETYPE.RF, EDGETYPE.FF },
            { EDGETYPE.RF, EDGETYPE.FR }, { EDGETYPE.RR, EDGETYPE.FF }, { EDGETYPE.RR, EDGETYPE.FR } };

    protected M outgoingMsg = null;
    protected VertexValueWritable tmpValue = new VertexValueWritable();
    protected VKmer repeatKmer = null; //for detect tandemRepeat
    protected EDGETYPE repeatEdgetype; //for detect tandemRepeat
    protected short outFlag;
    protected short selfFlag;

    protected boolean verbose = false;
    protected boolean logReadIds = false;

    private ResettableCounters counters = new ResettableCounters();

    final protected void resetCounters() {
        counters.reset();
    }

    public Counters getCounters() {
        return counters;
    }

    /**
     * initiate kmerSize, maxIteration
     */
    public void initVertex() {
        if (getSuperstep() == 1) {
            if (kmerSize == -1) {
                kmerSize = Integer.parseInt(getContext().getConfiguration().get(GenomixJobConf.KMER_LENGTH));
            }
            if (maxIteration < 0) {
                maxIteration = Integer.parseInt(getContext().getConfiguration().get(
                        GenomixJobConf.GRAPH_CLEAN_MAX_ITERATIONS));
            }
            GenomixJobConf.setGlobalStaticConstants(getContext().getConfiguration());
        }

        verbose = false;
        if (GenomixJobConf.debug) {
            for (VKmer debugKmer : GenomixJobConf.debugKmers) {
                verbose |= (getVertexValue().findEdge(debugKmer) != null || getVertexId().equals(debugKmer));
            }
        }
    }

    /**
     * Generate random string from [ACGT]
     */
    public String generaterRandomDNAString(int n) {
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
        job.setVertexClass(vertexClass);
        job.setCounterAggregatorClass(DeBruijnVertexCounterAggregator.class);
        job.setVertexInputFormatClass(NodeToVertexInputFormat.class);
        job.setVertexOutputFormatClass(VertexToNodeOutputFormat.class);
        job.setOutputKeyClass(VKmer.class);
        job.setOutputValueClass(Node.class);
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
            EDGETYPE[] edgeTypes = direction.edgeTypes();
            for (EDGETYPE et : edgeTypes) {
                if (getVertexValue().getEdges(et).size() > 0)
                    return getVertexValue().getEdges(et).getPosition(0);
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
        for (EDGETYPE et : EDGETYPE.values) {
            for (VKmer kmerToCheck : value.getEdges(et)) {
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
        for (EDGETYPE et : EDGETYPE.values) {
            for (VKmer dest : vertex.getEdges(et)) {
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
            getVertexValue().getEdges(meToNeighborEdgetype).remove(incomingMsg.getSourceVertexId());

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
            for (VKmer dest : vertex.getEdges(et)) {
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
    
    /**
     * update statistics for distribution
     */
    protected void updateStats(String valueName, long value) {
        getCounters().findCounter(valueName + "-bins", Long.toString(value)).increment(1);
    }
}