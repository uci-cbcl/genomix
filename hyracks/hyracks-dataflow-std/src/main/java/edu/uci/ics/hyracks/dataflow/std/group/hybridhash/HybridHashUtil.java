/*
 * Copyright 2009-2012 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.dataflow.std.group.hybridhash;

public class HybridHashUtil {

    /**
     * Compute the expected number of spilling partitions (in-memory partition is not included), using the hybrid-hash
     * algorithm from [Shapiro86]. Note that 0 means that there is no need to have spilling partitions.
     * 
     * @param inputSizeInFrames
     * @param memorySizeInFrames
     * @param fudgeFactor
     * @return
     */
    public static int hybridHashPartitionComputer(int inputSizeOfUniqueKeysInFrames, int memorySizeInFrames,
            double fudgeFactor) {
        return Math.max(
                (int) Math.ceil((inputSizeOfUniqueKeysInFrames * fudgeFactor - memorySizeInFrames)
                        / (memorySizeInFrames - 1)), 0);
    }

    /**
     * Compute the estimated number of unique keys in a partition of a dataset, using Yao's formula
     * 
     * @param inputSizeInRawRecords
     * @param inputSizeInUniqueKeys
     * @param numOfPartitions
     * @return
     */
    public static long getEstimatedPartitionSizeOfUniqueKeys(long inputSizeInRawRecords, long inputSizeInUniqueKeys,
            int numOfPartitions) {
        if (numOfPartitions == 1) {
            return inputSizeInUniqueKeys;
        }
        return (long) Math.ceil(inputSizeInUniqueKeys
                * (1 - Math.pow(1 - ((double) inputSizeInRawRecords / (double) numOfPartitions)
                        / (double) inputSizeInRawRecords, (double) inputSizeInRawRecords
                        / (double) inputSizeInUniqueKeys)));
    }
}
