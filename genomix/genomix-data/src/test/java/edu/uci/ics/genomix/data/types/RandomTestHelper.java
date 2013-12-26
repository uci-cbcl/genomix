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

import java.util.ArrayList;
import java.util.Map;
import java.util.Random;
import java.util.SortedSet;
import java.util.AbstractMap.SimpleEntry;

import junit.framework.Assert;

public class RandomTestHelper {
    private static final char[] symbols = new char[4];
    static {
        symbols[0] = 'A';
        symbols[1] = 'C';
        symbols[2] = 'G';
        symbols[3] = 'T';
    }

    public static String generateGeneString(int length) {
        Random random = new Random();
        char[] buf = new char[length];
        for (int idx = 0; idx < buf.length; idx++) {
            buf[idx] = symbols[random.nextInt(4)];
        }
        return new String(buf);
    }

    /**
     * generate a random int within the range [min, max]
     * 
     * @param min
     * @param max
     * @return
     */
    public static int genRandomInt(int min, int max) {
        return min + (int) (Math.random() * ((max - min) + 1));
    }

    /**
     * eg. GCTA will convert to TAGC
     * 
     * @param src
     * @return
     */
    public static String getFlippedGeneStr(String src) {
        int length = src.length();
        char[] input = new char[length];
        char[] output = new char[length];
        src.getChars(0, length, input, 0);
        for (int i = length - 1; i >= 0; i--) {
            switch (input[i]) {
                case 'A':
                    output[length - 1 - i] = 'T';
                    break;
                case 'C':
                    output[length - 1 - i] = 'G';
                    break;
                case 'G':
                    output[length - 1 - i] = 'C';
                    break;
                case 'T':
                    output[length - 1 - i] = 'A';
                    break;
            }
        }
        return new String(output);
    }

    public static void assembleNodeRandomly(Node targetNode, int orderNum, int min, int max) {
        String srcInternalStr = generateGeneString(orderNum);
        VKmer srcInternalKmer = new VKmer(srcInternalStr);
        VKmerList sampleList;
        for (EDGETYPE e : EDGETYPE.values()) {
            sampleList = new VKmerList();
            for (int i = 0; i < min + (int) (Math.random() * ((max - min) + 1)); i++) {
                String edgeStr = generateGeneString(orderNum);
                VKmer edgeKmer = new VKmer(edgeStr);
                sampleList.append(edgeKmer);
            }
            targetNode.setEdges(e, sampleList);
        }
        ReadHeadSet startReads = new ReadHeadSet();
        ReadHeadSet endReads = new ReadHeadSet();
        byte mateId;
        long readId;
        byte libraryId;
        int offset;
        ReadHeadInfo pos;
        for (long i = 0; i < min + (int) (Math.random() * ((max - min) + 1)); i++) {
            mateId = (byte) (0);
            readId = i;
            offset = (int) (i % (ReadHeadInfo.MAX_OFFSET_VALUE + 1));
            libraryId = (byte) (i % (ReadHeadInfo.MAX_LIBRARY_VALUE + 1));
            pos = new ReadHeadInfo(mateId, libraryId, readId, offset, null, null);
            startReads.add(pos);
        }
        for (long i = 0; i < min + (int) (Math.random() * ((max - min) + 1)); i++) {
            mateId = (byte) (1);
            readId = i;
            offset = (int) (i % (ReadHeadInfo.MAX_OFFSET_VALUE + 1));
            libraryId = (byte) (i % (ReadHeadInfo.MAX_LIBRARY_VALUE + 1));
            pos = new ReadHeadInfo(mateId, libraryId, readId, offset, null, null);
            endReads.add(pos);
        }
        targetNode.setUnflippedReadIds(startReads);
        targetNode.setFlippedReadIds(endReads);
        targetNode.setInternalKmer(srcInternalKmer);
        targetNode.setAverageCoverage((float) (orderNum * (min + (int) (Math.random() * ((max - min) + 1)))));
    }

    public static void printSrcNodeInfo(Node srcNode) {
        System.out.println("InternalKmer: " + srcNode.getInternalKmer().toString());
        for (EDGETYPE e : EDGETYPE.values()) {
            System.out.println(e.toString());
            for (VKmer iter : srcNode.getEdges(e)) {
                System.out.println("edgeKmer: " + iter.toString());
                System.out.println("");
            }
            System.out.println("-------------------------------------");
        }
        System.out.println("StartReads");
        for (ReadHeadInfo startIter : srcNode.getUnflippedReadIds().getOffSetRange(0, Integer.MAX_VALUE))
            System.out.println(startIter.toString() + "---");
        System.out.println("");
        System.out.println("EndsReads");
        for (ReadHeadInfo startIter : srcNode.getFlippedReadIds().getOffSetRange(0, Integer.MAX_VALUE))
            System.out.println(startIter.toString() + "---");
        System.out.println("");
        System.out.println("Coverage: " + srcNode.getAverageCoverage());
        System.out.println("***************************************");
    }

    public static void compareTwoNodes(Node et1, Node et2) {
        Assert.assertEquals(et1.getInternalKmer().toString(), et2.getInternalKmer().toString());
        for (EDGETYPE e : EDGETYPE.values()) {
            Assert.assertEquals(et1.getEdges(e).size(), et2.getEdges(e).size());
            for (int i = 0; i < et1.getEdges(e).size(); i++) {
                VKmer iter1 = et1.getEdges(e).getPosition(i);
                VKmer iter2 = et2.getEdges(e).getPosition(i);
                Assert.assertEquals(iter1.toString(), iter2.toString());
            }
        }
        for (ReadHeadInfo startIter1 : et1.getUnflippedReadIds().getOffSetRange(0, Integer.MAX_VALUE)){
            ReadHeadInfo startIter2 = et2.getUnflippedReadIds().getOffSetRange(0, Integer.MAX_VALUE).first();
            Assert.assertEquals(startIter1.toString(), startIter2.toString());
            et2.getUnflippedReadIds().getOffSetRange(0, Integer.MAX_VALUE).remove(startIter2);
        }
        for (ReadHeadInfo startIter1 : et1.getFlippedReadIds().getOffSetRange(0, Integer.MAX_VALUE)){
            ReadHeadInfo startIter2 = et2.getFlippedReadIds().getOffSetRange(0, Integer.MAX_VALUE).first();
            Assert.assertEquals(startIter1.toString(), startIter2.toString());
            et2.getUnflippedReadIds().getOffSetRange(0, Integer.MAX_VALUE).remove(startIter2);
        }
    }
}
