package edu.uci.ics.genomix.pregelix.operator.removelowcoverage;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.genomix.data.types.ReadHeadInfo;
import edu.uci.ics.genomix.pregelix.base.MessageWritable;

public class ShiftedReadSetMessage extends MessageWritable {

    private ReadHeadInfo shiftedInfo = new ReadHeadInfo();

    public void setReadInfo(ReadHeadInfo info) {
        shiftedInfo = info;
    }

    public ReadHeadInfo getReadInfo() {
        return shiftedInfo;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        shiftedInfo.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        shiftedInfo.write(out);
    }
}
