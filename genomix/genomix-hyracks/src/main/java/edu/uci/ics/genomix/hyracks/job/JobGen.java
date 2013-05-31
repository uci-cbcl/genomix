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

package edu.uci.ics.genomix.hyracks.job;

import java.io.Serializable;
import java.util.UUID;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.hdfs.dataflow.ConfFactory;

public abstract class JobGen implements Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    protected final ConfFactory confFactory;
    protected String jobId = new UUID(System.currentTimeMillis(), System.nanoTime()).toString();

    public JobGen(GenomixJobConf job) throws HyracksDataException {
        this.confFactory = new ConfFactory(job);
    }

    public abstract JobSpecification generateJob() throws HyracksException;
}
