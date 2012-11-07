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
package edu.uci.ics.hyracks.dataflow.std.group.hybridhash.prepart;

import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public interface IHashHashSpillableFrameWriter extends IFrameWriter {

    /**
     * Get the spilled runs generated during the write.
     * 
     * @return
     */
    public List<IFrameReader> getSpilledRuns() throws HyracksDataException;

    /**
     * Get the size of spilled runs generated during the write.
     * 
     * @return
     */
    public List<Integer> getSpilledRunsSizeInPages() throws HyracksDataException;

    public List<Integer> getSpilledRunsSizeInTuples() throws HyracksDataException;

    /**
     * Get the size (in pages) of the direct flushed part.
     * 
     * @return
     * @throws HyracksDataException
     */
    public int getDirectFlushSizeInPages() throws HyracksDataException;

    public int getDirectFlushSizeInTuples() throws HyracksDataException;

    public int getDirectFlushRawSizeInTuples() throws HyracksDataException;

    public void finishup() throws HyracksDataException;

}
