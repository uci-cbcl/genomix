package edu.uci.ics.genomix.pregelix.log;

import java.util.logging.*;

import edu.uci.ics.genomix.pregelix.io.message.MessageWritable;
import edu.uci.ics.genomix.type.KmerBytesWritable;

public class LogAlgorithmLogFormatter extends Formatter {
    //
    // Create a DateFormat to format the logger timestamp.
    //
    private long step;
    private KmerBytesWritable sourceVertexId = new KmerBytesWritable();
    private KmerBytesWritable destVertexId = new KmerBytesWritable();
    private KmerBytesWritable mergeChain = new KmerBytesWritable();
    //private boolean testDelete = false;
    /**
     * 0: general operation
     * 1: testDelete
     * 2: testMergeChain
     * 3: testVoteToHalt
     */
    private int operation;

    public LogAlgorithmLogFormatter() {
    }

    public void set(long step, KmerBytesWritable sourceVertexId, KmerBytesWritable destVertexId,
            MessageWritable msg, byte state) {
        this.step = step;
        this.sourceVertexId.setAsCopy(sourceVertexId);
        this.destVertexId.setAsCopy(destVertexId);
        this.operation = 0;
    }

    public void setMergeChain(long step, KmerBytesWritable sourceVertexId, KmerBytesWritable mergeChain) {
        this.reset();
        this.step = step;
        this.sourceVertexId.setAsCopy(sourceVertexId);
        this.mergeChain.setAsCopy(mergeChain);
        this.operation = 2;
    }

    public void setVotoToHalt(long step, KmerBytesWritable sourceVertexId) {
        this.reset();
        this.step = step;
        this.sourceVertexId.setAsCopy(sourceVertexId);
        this.operation = 3;
    }

    public void reset() {
        this.sourceVertexId = new KmerBytesWritable();
        this.destVertexId = new KmerBytesWritable();
        this.mergeChain = new KmerBytesWritable();
    }

    public String format(LogRecord record) {
        StringBuilder builder = new StringBuilder(1000);
        String source = sourceVertexId.toString();
        String chain = "";

        builder.append("Step: " + step + "\r\n");
        builder.append("Source Code: " + source + "\r\n");
        if (operation == 0) {
            if (KmerBytesWritable.getKmerLength() != -1) {
                String dest = destVertexId.toString();
                builder.append("Send message to " + "\r\n");
                builder.append("Destination Code: " + dest + "\r\n");
            }

        }
        if (operation == 2) {
            chain = mergeChain.toString();
            builder.append("Merge Chain: " + chain + "\r\n");
            builder.append("Merge Chain Length: " + KmerBytesWritable.getKmerLength() + "\r\n");
        }
        if (operation == 3)
            builder.append("Vote to halt!");
        if (!formatMessage(record).equals(""))
            builder.append(formatMessage(record) + "\r\n");
        builder.append("\n");
        return builder.toString();
    }

    public String getHead(Handler h) {
        return super.getHead(h);
    }

    public String getTail(Handler h) {
        return super.getTail(h);
    }

    public int getOperation() {
        return operation;
    }

    public void setOperation(int operation) {
        this.operation = operation;
    }
}