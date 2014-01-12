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
package edu.uci.ics.genomix.data.types;

import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Iterator;

import junit.framework.Assert;

import org.junit.Test;

import edu.uci.ics.genomix.data.types.Node.NeighborInfo;
import edu.uci.ics.genomix.data.types.Node.READHEAD_ORIENTATION;

public class NodeFixedTest {

    @Test
    public void testNeighborsInfo() throws IOException {
        String str1 = "ATGCATGCGCTAG";
        VKmer vkmer1 = new VKmer(str1);
        String str2 = "ATGCATCCCCTAG";
        VKmer vkmer2 = new VKmer(str2);
        String str3 = "AGGGATGCGCTAG";
        VKmer vkmer3 = new VKmer(str3);
        String str4 = "ATGCATAAATAC";
        VKmer vkmer4 = new VKmer(str4);
        VKmerList source = new VKmerList();
        source.append(vkmer1);
        source.append(vkmer2);
        source.append(vkmer3);
        source.append(vkmer4);
        Node.NeighborsInfo neighborsInfor = new Node.NeighborsInfo(EDGETYPE.FF, source);
        Iterator<NeighborInfo> iterator = neighborsInfor.iterator();
        NeighborInfo temp = iterator.next();
        Assert.assertEquals(EDGETYPE.FF, temp.et);
        Assert.assertEquals(str1, temp.kmer.toString());
        temp = iterator.next();
        Assert.assertEquals(str2, temp.kmer.toString());
        temp = iterator.next();
        Assert.assertEquals(str3, temp.kmer.toString());
        temp = iterator.next();
        Assert.assertEquals(str4, temp.kmer.toString());
    }

    /**
     * basic checking for enum DIR in Node class
     * 
     * @throws IOException
     */
    @Test
    public void testDIR() throws IOException {
        Assert.assertEquals(0b01 << 2, DIR.REVERSE.get());
        Assert.assertEquals(0b10 << 2, DIR.FORWARD.get());
        DIR testDir1 = DIR.FORWARD;
        DIR testDir2 = DIR.REVERSE;
        Assert.assertEquals(DIR.REVERSE, testDir1.mirror());
        Assert.assertEquals(DIR.FORWARD, testDir2.mirror());
        Assert.assertEquals(0b11 << 2, DIR.fromSet(EnumSet.allOf(DIR.class)));
        Assert.assertEquals(0b00 << 2, DIR.fromSet(EnumSet.noneOf(DIR.class)));

        EDGETYPE[] edgeTypes1 = testDir1.edgeTypes();
        EnumSet<EDGETYPE> edgeExample1 = EnumSet.noneOf(EDGETYPE.class);
        EDGETYPE[] edgeTypes2 = testDir2.edgeTypes();
        EnumSet<EDGETYPE> edgeExample2 = EnumSet.noneOf(EDGETYPE.class);
        edgeExample1.add(EDGETYPE.FF);
        edgeExample1.add(EDGETYPE.FR);
        Assert.assertEquals(edgeExample1, EnumSet.copyOf(Arrays.asList(edgeTypes1)));

        edgeExample2.add(EDGETYPE.RF);
        edgeExample2.add(EDGETYPE.RR);
        Assert.assertEquals(edgeExample2, EnumSet.copyOf(Arrays.asList(edgeTypes2)));

        Assert.assertEquals(edgeExample1, EnumSet.copyOf(Arrays.asList(DIR.edgeTypesInDir(testDir1))));
        Assert.assertEquals(edgeExample2, EnumSet.copyOf(Arrays.asList(DIR.edgeTypesInDir(testDir2))));

        EnumSet<DIR> dirExample = EnumSet.noneOf(DIR.class);
        dirExample.add(DIR.FORWARD);
        Assert.assertEquals(dirExample, DIR.enumSetFromByte((short) 8));
        dirExample.clear();
        dirExample.add(DIR.REVERSE);
        Assert.assertEquals(dirExample, DIR.enumSetFromByte((short) 4));

        dirExample.clear();
        dirExample.add(DIR.FORWARD);
        Assert.assertEquals(dirExample, DIR.flipSetFromByte((short) 4));
        dirExample.clear();
        dirExample.add(DIR.REVERSE);
        Assert.assertEquals(dirExample, DIR.flipSetFromByte((short) 8));
    }

    /**
     * basic checking for EDGETYPE in Node class
     * 
     * @throws IOException
     */
    @Test
    public void testEDGETYPE() throws IOException {
        //fromByte()
        Assert.assertEquals(EDGETYPE.FF, EDGETYPE.fromByte((byte) 0));
        Assert.assertEquals(EDGETYPE.FR, EDGETYPE.fromByte((byte) 1));
        Assert.assertEquals(EDGETYPE.RF, EDGETYPE.fromByte((byte) 2));
        Assert.assertEquals(EDGETYPE.RR, EDGETYPE.fromByte((byte) 3));
        //mirror()
        Assert.assertEquals(EDGETYPE.RR, EDGETYPE.FF.mirror());
        Assert.assertEquals(EDGETYPE.FR, EDGETYPE.FR.mirror());
        Assert.assertEquals(EDGETYPE.RF, EDGETYPE.RF.mirror());
        Assert.assertEquals(EDGETYPE.FF, EDGETYPE.RR.mirror());
        //DIR()
        Assert.assertEquals(DIR.FORWARD, EDGETYPE.FF.dir());
        Assert.assertEquals(DIR.FORWARD, EDGETYPE.FR.dir());
        Assert.assertEquals(DIR.REVERSE, EDGETYPE.RF.dir());
        Assert.assertEquals(DIR.REVERSE, EDGETYPE.RR.dir());
        //resolveEdgeThroughPath()
        Assert.assertEquals(EDGETYPE.RF,
                EDGETYPE.resolveEdgeThroughPath(EDGETYPE.fromByte((byte) 0), EDGETYPE.fromByte((byte) 2)));
        Assert.assertEquals(EDGETYPE.RR,
                EDGETYPE.resolveEdgeThroughPath(EDGETYPE.fromByte((byte) 0), EDGETYPE.fromByte((byte) 3)));

        Assert.assertEquals(EDGETYPE.FF,
                EDGETYPE.resolveEdgeThroughPath(EDGETYPE.fromByte((byte) 1), EDGETYPE.fromByte((byte) 2)));
        Assert.assertEquals(EDGETYPE.FR,
                EDGETYPE.resolveEdgeThroughPath(EDGETYPE.fromByte((byte) 1), EDGETYPE.fromByte((byte) 3)));

        Assert.assertEquals(EDGETYPE.RF,
                EDGETYPE.resolveEdgeThroughPath(EDGETYPE.fromByte((byte) 2), EDGETYPE.fromByte((byte) 0)));
        Assert.assertEquals(EDGETYPE.RR,
                EDGETYPE.resolveEdgeThroughPath(EDGETYPE.fromByte((byte) 2), EDGETYPE.fromByte((byte) 1)));

        Assert.assertEquals(EDGETYPE.FF,
                EDGETYPE.resolveEdgeThroughPath(EDGETYPE.fromByte((byte) 3), EDGETYPE.fromByte((byte) 0)));
        Assert.assertEquals(EDGETYPE.FR,
                EDGETYPE.resolveEdgeThroughPath(EDGETYPE.fromByte((byte) 3), EDGETYPE.fromByte((byte) 1)));
        //causeFlip()
        Assert.assertEquals(false, EDGETYPE.FF.causesFlip());
        Assert.assertEquals(true, EDGETYPE.FR.causesFlip());
        Assert.assertEquals(true, EDGETYPE.RF.causesFlip());
        Assert.assertEquals(false, EDGETYPE.RR.causesFlip());
        //flipNeighbor()
        Assert.assertEquals(true, EDGETYPE.sameOrientation(EDGETYPE.RF, EDGETYPE.FR));
        Assert.assertEquals(false, EDGETYPE.sameOrientation(EDGETYPE.RF, EDGETYPE.RR));
    }

    @Test
    public void testREADHEAD_ORIENTATION() throws IOException {
        Assert.assertEquals(READHEAD_ORIENTATION.FLIPPED, READHEAD_ORIENTATION.fromByte((byte) 1));
        Assert.assertEquals(READHEAD_ORIENTATION.UNFLIPPED, READHEAD_ORIENTATION.fromByte((byte) 0));
    }
}
