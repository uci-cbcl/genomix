package edu.uci.ics.genomix.data.std.accessors;

import edu.uci.ics.genomix.data.std.primitive.KmerPointable;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunction;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;

public class KmerBinaryHashFunctionFamily implements IBinaryHashFunctionFamily {
	private static final long serialVersionUID = 1L;

	@Override
	public IBinaryHashFunction createBinaryHashFunction(final int seed) {

		return new IBinaryHashFunction() {
			private KmerPointable p = new KmerPointable();
			
			@Override
			public int hash(byte[] bytes, int offset, int length) {
				if (length + offset >= bytes.length)
					throw new IllegalStateException("out of bound");
				p.set(bytes, offset, length);
				int hash = p.hash() * (seed + 1);
				if (hash < 0) {
					hash = -(hash+1);
				}
				return hash;
			}
		};
	}
}