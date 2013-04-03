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

package edu.uci.ics.hyracks.api.job;

import java.net.URL;
import java.util.List;

import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.util.JavaSerializationUtils;

public class JobSerializerDeserializer implements IJobSerializerDeserializer {

    @Override
    public JobSpecification deserializeJobSpecification(byte[] jsBytes) throws HyracksException {
        try {
            return (JobSpecification) JavaSerializationUtils.deserialize(jsBytes);
        } catch (Exception e) {
            throw new HyracksException(e);
        }
    }

    @Override
    public byte[] serializeJobSpecication(JobSpecification jobSpec) throws HyracksException {
        try {
            return JavaSerializationUtils.serialize(jobSpec);
        } catch (Exception e) {
            throw new HyracksException(e);
        }
    }

    @Override
    public ActivityClusterGraph deserializeActivityClusterGraph(byte[] acgBytes) throws HyracksException {
        try {
            return (ActivityClusterGraph) JavaSerializationUtils.deserialize(acgBytes);
        } catch (Exception e) {
            throw new HyracksException(e);
        }
    }

    @Override
    public byte[] serializeActivityClusterGraph(ActivityClusterGraph acg) throws HyracksException {
        try {
            return JavaSerializationUtils.serialize(acg);
        } catch (Exception e) {
            throw new HyracksException(e);
        }
    }

    @Override
    public void addClassPathURLs(List<URL> binaryURLs) {
        throw new UnsupportedOperationException("Not supported by " + this.getClass().getName());
    }

}
