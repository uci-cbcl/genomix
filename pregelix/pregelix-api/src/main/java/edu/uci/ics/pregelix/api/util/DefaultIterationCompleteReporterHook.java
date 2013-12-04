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
package edu.uci.ics.pregelix.api.util;

import edu.uci.ics.pregelix.api.job.IIterationCompleteReporterHook;
import edu.uci.ics.pregelix.api.job.PregelixJob;

/**
 * The default iteration complete reporter hook does nothing
 * 
 * @author wbiesing
 */
public class DefaultIterationCompleteReporterHook implements IIterationCompleteReporterHook {

    @Override
    public void completeIteration(int superstep, PregelixJob job) {
        System.out.println("iteration complete reporter for " + superstep + " job " + job);
    }

}
