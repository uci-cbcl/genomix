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

package edu.uci.ics.genomix.data.config;

import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;
import org.junit.Assert;
import org.kohsuke.args4j.CmdLineException;

import edu.uci.ics.genomix.data.config.GenomixJobConf;

public class GenomixJobConfTest {
    
    @SuppressWarnings({ "deprecation", "unused" })
    @Test
    public void testDefaultArgs() {
        try {
            JobConf other = new JobConf();
            other.setInt(GenomixJobConf.KMER_LENGTH, 5);
            GenomixJobConf conf = new GenomixJobConf(other);
        } catch (IllegalArgumentException e) {
            Assert.fail("Config should have been okay!");
        }
        
        try {
            GenomixJobConf conf = new GenomixJobConf(-1);
            Assert.fail("Should have thrown an exception! (Missing KMER_LENGTH)");
        } catch (IllegalArgumentException e) {
            Assert.assertEquals("kmerLength is unset!", e.getMessage());
        }
        
        try {
            GenomixJobConf conf = new GenomixJobConf(2);
            Assert.fail("Should have thrown an exception! (short KMER_LENGTH)");
        } catch (IllegalArgumentException e) {
            Assert.assertEquals("kmerLength must be at least 3!", e.getMessage());
        }
        
        try {
            GenomixJobConf conf = new GenomixJobConf(5);
        } catch (IllegalArgumentException e) {
            Assert.fail("Config should have been correct!");
        }
        
        try {
            JobConf other = new JobConf();
            GenomixJobConf conf = new GenomixJobConf(other);
            Assert.fail("Should have failed (MISSING KMER_LENGTH)!");
        } catch (IllegalArgumentException e) {
            
        }
        
        try {
            JobConf other = new JobConf();
            other.setInt(GenomixJobConf.KMER_LENGTH, 2);
            GenomixJobConf conf = new GenomixJobConf(other);
            Assert.fail("Should have failed (SHORT KMER_LENGTH)!");
        } catch (IllegalArgumentException e) {
            
        }

    }
    
    @Test
    public void testFromArgs() {
        try {
            String[] args = {"-kmerLength", "5", "-localInput", "file1", "-readLengths", "22"};
            GenomixJobConf.fromArguments(args);
        } catch(CmdLineException e) {
            Assert.fail("Config was valid-- shouldn't have thrown an error! Error was: " + e.getMessage());
        }
        
        try {
            String[] args = {"-kmerLength", "2", "-localInput", "file1", "-readLengths", "22"};
            GenomixJobConf.fromArguments(args);
            Assert.fail("Should have thrown an exception (SHORT kmer)");
        } catch(IllegalArgumentException e) {
        } catch (CmdLineException e) {
        }
        
        try {
            String[] args = {"-localInput", "file1"};
            GenomixJobConf.fromArguments(args);
            Assert.fail("Should have thrown an exception! (Missing Kmer!)");
        } catch(CmdLineException e) {
        }
        
    }
    
}
