package edu.uci.ics.genomix.data.std.accessors;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunction;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import edu.uci.ics.hyracks.data.std.api.IHashable;
import edu.uci.ics.hyracks.data.std.primitive.LongPointable;

public class LongBinaryHashFunctionFamily implements IBinaryHashFunctionFamily {
	private static final long serialVersionUID = 1L;

	@Override
	public IBinaryHashFunction createBinaryHashFunction(final int seed) {

		return new IBinaryHashFunction() {
			private LongPointable p = new LongPointable();

			@Override
			public int hash(byte[] bytes, int offset, int length) {
				if (length + offset >= bytes.length)
					throw new IllegalStateException("out of bound");
				p.set(bytes, offset, length);
				int hash = Math.abs(((IHashable) p).hash() * (seed + 1));
				if (hash < 0) {
					hash = Math.abs(hash + 1);
				}
				return hash;
			}
		};
	}
}
