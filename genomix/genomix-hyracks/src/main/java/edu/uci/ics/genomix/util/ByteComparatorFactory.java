package edu.uci.ics.genomix.util;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunction;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;

public class ByteComparatorFactory implements IBinaryComparatorFactory, IBinaryHashFunctionFactory {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public IBinaryComparator createBinaryComparator() {
		return new IBinaryComparator(){

			@Override
			public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2,
					int l2) {
				return b1[s1]-b2[s2];
			}
			
		};
	}

	@Override
	public IBinaryHashFunction createBinaryHashFunction() {
		return new IBinaryHashFunction(){

			@Override
			public int hash(byte[] bytes, int offset, int length) {
				return bytes[offset];
			}
			
		};
	}

}
