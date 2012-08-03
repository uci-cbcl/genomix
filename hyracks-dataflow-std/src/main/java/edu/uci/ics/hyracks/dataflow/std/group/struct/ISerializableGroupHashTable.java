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
package edu.uci.ics.hyracks.dataflow.std.group.struct;

import java.util.LinkedList;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileReader;

public interface ISerializableGroupHashTable {

    public void insert(FrameTupleAccessor accessor, int tupleIndex) throws HyracksDataException;

    public LinkedList<RunFileReader> getRunFileReaders() throws HyracksDataException;

    public void reset() throws HyracksDataException;

    public void flushHashtableToOutput(IFrameWriter outputWriter) throws HyracksDataException;

    public void finishup() throws HyracksDataException;

    public void close() throws HyracksDataException;

    public int getTupleCount();

    public int getFrameSize();

}
