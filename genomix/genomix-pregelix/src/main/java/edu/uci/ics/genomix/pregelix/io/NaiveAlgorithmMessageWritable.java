package edu.uci.ics.genomix.pregelix.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import edu.uci.ics.genomix.pregelix.operator.NaiveAlgorithmForPathMergeVertex;
import edu.uci.ics.genomix.pregelix.type.CheckMessage;
import edu.uci.ics.genomix.pregelix.type.Message;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;

public class NaiveAlgorithmMessageWritable implements WritableComparable<NaiveAlgorithmMessageWritable> {
    /**
     * sourceVertexId stores source vertexId when headVertex sends the message
     * stores neighber vertexValue when pathVertex sends the message
     * file stores the point to the file that stores the chains of connected DNA
     */
    private KmerBytesWritable sourceVertexId;
    private byte adjMap;
    private byte lastGeneCode;
    private byte message;

    private byte checkMessage;

    public NaiveAlgorithmMessageWritable() {
        sourceVertexId = new VKmerBytesWritable(NaiveAlgorithmForPathMergeVertex.kmerSize);
        adjMap = (byte) 0;
        lastGeneCode = (byte) 0;
        message = Message.NON;
        checkMessage = (byte) 0;
    }

    public void set(KmerBytesWritable sourceVertex, byte adjMap, byte lastGeneCode, byte message) {
        checkMessage = 0;
        if (sourceVertexId != null) {
            checkMessage |= CheckMessage.SOURCE;
            this.sourceVertexId.set(sourceVertexId);
        }
        if (adjMap != 0) {
            checkMessage |= CheckMessage.ADJMAP;
            this.adjMap = adjMap;
        }
        if (lastGeneCode != 0) {
            checkMessage |= CheckMessage.LASTGENECODE;
            this.lastGeneCode = lastGeneCode;
        }
        this.message = message;
    }

    public void reset() {
        checkMessage = 0;
        adjMap = (byte) 0;
        lastGeneCode = (byte) 0;
        message = Message.NON;
    }

    public KmerBytesWritable getSourceVertexId() {
        return sourceVertexId;
    }

    public void setSourceVertexId(KmerBytesWritable sourceVertexId) {
        if (sourceVertexId != null) {
            checkMessage |= CheckMessage.SOURCE;
            this.sourceVertexId.set(sourceVertexId);
        }
    }

    public byte getAdjMap() {
        return adjMap;
    }

    public void setAdjMap(byte adjMap) {
        if (adjMap != 0) {
            checkMessage |= CheckMessage.ADJMAP;
            this.adjMap = adjMap;
        }
    }

    public byte getLastGeneCode() {
        return lastGeneCode;
    }

    public void setLastGeneCode(byte lastGeneCode) {
        if (lastGeneCode != 0) {
            checkMessage |= CheckMessage.LASTGENECODE;
            this.lastGeneCode = lastGeneCode;
        }
    }

    public byte getMessage() {
        return message;
    }

    public void setMessage(byte message) {
        this.message = message;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeByte(checkMessage);
        if ((checkMessage & CheckMessage.SOURCE) != 0)
            sourceVertexId.write(out);
        if ((checkMessage & CheckMessage.ADJMAP) != 0)
            out.write(adjMap);
        if ((checkMessage & CheckMessage.LASTGENECODE) != 0)
            out.write(lastGeneCode);
        out.write(message);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.reset();
        checkMessage = in.readByte();
        if ((checkMessage & CheckMessage.SOURCE) != 0)
            sourceVertexId.readFields(in);
        if ((checkMessage & CheckMessage.ADJMAP) != 0)
            adjMap = in.readByte();
        if ((checkMessage & CheckMessage.LASTGENECODE) != 0)
            lastGeneCode = in.readByte();
        message = in.readByte();
    }

    @Override
    public int hashCode() {
        return sourceVertexId.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof NaiveAlgorithmMessageWritable) {
            NaiveAlgorithmMessageWritable tp = (NaiveAlgorithmMessageWritable) o;
            return sourceVertexId.equals(tp.sourceVertexId);
        }
        return false;
    }

    @Override
    public String toString() {
        return sourceVertexId.toString();
    }

    @Override
    public int compareTo(NaiveAlgorithmMessageWritable tp) {
        return sourceVertexId.compareTo(tp.sourceVertexId);
    }
}
