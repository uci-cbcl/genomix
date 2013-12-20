package edu.uci.ics.pregelix.core.util;

import org.apache.hadoop.io.WritableComparable;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.pregelix.core.data.TypeTraits;
import edu.uci.ics.pregelix.core.jobgen.JobGenUtil;
import edu.uci.ics.pregelix.dataflow.util.ChunkId;

public class TreeIndexTypeUtils {

    // TODO find a way to associate with runtime pagesize;
    public static final int CHUNK_SIZE = 64 * 1024;

    public static ITypeTraits[] TypeTraits = { new TypeTraits(false), ChunkId.TypeTrait, new TypeTraits(false) };

    public static int[] KeyFields = new int[] { 0, 1 };

    public static int[] FieldPermutation = new int[] { 0, 1, 2 };

    public static IBinaryComparatorFactory[] getBinaryComparatorFactories(
            Class<? extends WritableComparable<?>> vertexIdClass) {
        IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[2];
        comparatorFactories[0] = JobGenUtil.getIBinaryComparatorFactory(0, vertexIdClass);
        comparatorFactories[1] = ChunkId.BinaryComparatorFactory;
        return comparatorFactories;
    }

}
