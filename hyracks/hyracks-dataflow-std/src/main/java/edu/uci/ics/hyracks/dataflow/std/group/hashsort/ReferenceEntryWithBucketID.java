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
package edu.uci.ics.hyracks.dataflow.std.group.hashsort;

import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.std.util.ReferenceEntry;

public class ReferenceEntryWithBucketID extends ReferenceEntry {

    private int bucketID;

    public ReferenceEntryWithBucketID(int runid, FrameTupleAccessor fta, int tupleIndex, int bucketID) {
        super(runid, fta, tupleIndex);
        this.bucketID = bucketID;
    }

    public int getBucketID() {
        return bucketID;
    }

    public void setBucketID(int bucketID) {
        this.bucketID = bucketID;
    }

}
