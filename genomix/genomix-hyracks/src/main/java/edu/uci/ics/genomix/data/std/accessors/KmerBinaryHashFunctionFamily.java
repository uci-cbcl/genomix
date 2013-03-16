package edu.uci.ics.genomix.data.std.accessors;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunction;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;

public class KmerBinaryHashFunctionFamily implements IBinaryHashFunctionFamily {
	private static final long serialVersionUID = 1L;

	@Override
	public IBinaryHashFunction createBinaryHashFunction(final int seed) {

		return new IBinaryHashFunction() {

			@Override
			public int hash(byte[] bytes, int offset, int length) {
				return KmerHashPartitioncomputerFactory.hashBytes(bytes,
						offset, length);
			}
		};
	}
}