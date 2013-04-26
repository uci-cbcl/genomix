/*
 * Copyright 2009-2012 by The Regents of the University of California
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
package edu.uci.ics.genomix.pathmergingh2;

import java.io.File;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;

public class MergePathMultiTextOutputFormat extends MultipleTextOutputFormat<Text, Text>{
    @Override
    protected String generateLeafFileName(String name) {
        // TODO Auto-generated method stub System.out.println(name); 
        String[] names = name.split("-");
        return names[0] + File.separator + name;
    }
}
