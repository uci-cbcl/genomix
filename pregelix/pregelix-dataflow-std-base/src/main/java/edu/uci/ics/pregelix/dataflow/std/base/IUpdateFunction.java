/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.pregelix.dataflow.std.base;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;

public interface IUpdateFunction extends IFunction {

    /**
     * update the tuple pointed by tupleRef called after process,
     * one-input-tuple-at-a-time
     * if the tuple is fixed length, the updated vertex value will directly write in-place, and mark the cursor as updated.
     * else the (vertexID, vertex) pair will write into the cloneUpdateTb.
     * 
     * @param tupleRef
     *            the tuple pointer
     * @param cloneUpdateTb
     *            the buffer to store the copied (vertexid, vertex) pair
     * @param cursor
     *            the original cursor inside the TreeIndex to update the vertex value in-place.
     * @throws HyracksDataException
     */
    public void update(ITupleReference tupleRef, ArrayTupleBuilder cloneUpdateTb, IIndexCursor cursor)
            throws HyracksDataException;

}
