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

package edu.uci.ics.pregelix.dataflow.util;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;

public class CopyUpdateUtil {

    /**
     * accumulate the batch updates from {@code cloneUpdateTb} into {@code updateBuffer}.
     * once the {@code updateBuffer} is full, it will process the batched updates onto {@code indexAccessor}.
     * and then it will reset the {@code cursor} to point to the same {@code frameTuple} inside the {@code indexAccessor} under the same {@code rangePred};
     * 
     * @param tempTupleReference
     *            the pre-allocated working tuple which copied the {@code frameTuple} before update.
     * @param frameTuple
     *            the tuple stored inside the tree.
     * @param updateBuffer
     *            the buffer to store a certain number of update request on the index.
     * @param cloneUpdateTb
     *            the tuple which stored one update information produced by the current {@code frameTuple}
     * @param indexAccessor
     *            the index
     * @param cursor
     *            the cursor pointing to the vertex inside the index.
     * @param rangePred
     *            the pre-allocated object to recover the cursor once the index is changed.
     * @param scan
     *            to clarify if the recovering cursor is a scan cursor
     * @param type
     *            the type of the index
     * @throws HyracksDataException
     * @throws IndexException
     */
    public static void copyUpdate(SearchKeyTupleReference tempTupleReference, ITupleReference frameTuple,
            UpdateBuffer updateBuffer, ArrayTupleBuilder cloneUpdateTb, IIndexAccessor indexAccessor,
            IIndexCursor cursor, RangePredicate rangePred, boolean scan, StorageType type) throws HyracksDataException,
            IndexException {
        if (cloneUpdateTb.getSize() > 0) {
            if (!updateBuffer.appendTuple(cloneUpdateTb)) {
                updateIndex(tempTupleReference, frameTuple, cursor, updateBuffer, indexAccessor, cloneUpdateTb);
                resetCursor(cursor, frameTuple, rangePred, scan, indexAccessor);
            }
            cloneUpdateTb.reset();
        }
    }

    private static void updateIndex(SearchKeyTupleReference tempTupleReference, ITupleReference frameTuple,
            IIndexCursor cursor, UpdateBuffer updateBuffer, IIndexAccessor indexAccessor,
            ArrayTupleBuilder cloneUpdateTb) throws HyracksDataException, IndexException {
        tempTupleReference.reset(frameTuple.getFieldData(0), frameTuple.getFieldStart(0), frameTuple.getFieldLength(0));
        //release the cursor/latch
        cursor.close();
        //batch update
        updateBuffer.updateIndex(indexAccessor);
        //try append the to-be-updated tuple again
        if (!updateBuffer.appendTuple(cloneUpdateTb)) {
            throw new HyracksDataException("cannot append tuple builder!");
        }
    }

    private static void resetCursor(IIndexCursor cursor, ITupleReference tempTupleReference, RangePredicate rangePred,
            boolean scan, IIndexAccessor indexAccessor) throws HyracksDataException, IndexException {
        //search again and recover the cursor to the exact point as the one before it is closed
        cursor.reset();
        rangePred.setLowKey(tempTupleReference, true);
        if (scan) {
            rangePred.setHighKey(null, true);
        } else {
            rangePred.setHighKey(tempTupleReference, true);
        }
        indexAccessor.search(cursor, rangePred);
        if (cursor.hasNext()) {
            cursor.next();
        }
    }

    public static void copyUpdateChunks(RecoverChunkedTupleBuilder recoverBuilder,
            SearchKeyTupleReference tempTupleReference, ITupleReference frameTuple, UpdateBuffer updateBuffer,
            ArrayTupleBuilder cloneUpdateTb, IIndexAccessor indexAccessor, IIndexCursor cursor,
            RangePredicate rangePred, boolean scan, StorageType type) throws HyracksDataException, IndexException {
        if (cloneUpdateTb.getSize() > 0) {
            if (!updateBuffer.appendTuple(cloneUpdateTb)) {
                updateIndex(tempTupleReference, frameTuple, cursor, updateBuffer, indexAccessor, cloneUpdateTb);
                resetChunkedCursor(recoverBuilder, cursor, frameTuple, rangePred, scan, indexAccessor);
            }
            cloneUpdateTb.reset();
        }
    }

    private static void resetChunkedCursor(RecoverChunkedTupleBuilder recoverBuilder, IIndexCursor cursor,
            ITupleReference tempTupleReference, RangePredicate rangePred, boolean scan, IIndexAccessor indexAccessor)
            throws HyracksDataException, IndexException {
        //search again and recover the cursor to the exact point as the one before it is closed
        cursor.reset();
        rangePred.setLowKey(tempTupleReference, true);
        if (scan) {
            rangePred.setHighKey(null, true);
        } else {
            rangePred.setHighKey(tempTupleReference, true);
        }
        indexAccessor.search(cursor, rangePred);
        recoverBuilder.reset(tempTupleReference);
        while (cursor.hasNext()) {
            cursor.next();
            if (!recoverBuilder.isNextChunk(cursor.getTuple())) {
                break;
            }
        }
    }
}
