package edu.uci.ics.genomix.pregelix.operator.scaffolding2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.genomix.pregelix.base.MessageWritable;
import edu.uci.ics.genomix.pregelix.operator.scaffolding2.RayScores.Rules;

public class PruneMessage extends MessageWritable {
	
	Rules rules;

	@Override
	public void reset() {
		super.reset();
		this.rules = null;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		if (in.readBoolean()) {
			rules = new Rules();
			rules.readFields(in);
		} else {
			rules = null;
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeBoolean(rules != null);
		if (rules != null) {
			rules.write(out);
		}
	}
}
