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
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;

import junit.framework.Assert;

import org.junit.Test;

public class VKmerListFixedTest {

    static byte[] array = { 'A', 'A', 'T', 'A', 'G', 'A', 'A', 'G' };

    public static String generaterRandomString(int n) {
        char[] chars = "ACGT".toCharArray();
        StringBuilder sb = new StringBuilder();
        Random random = new Random();
        for (int i = 0; i < n; i++) {
            char c = chars[random.nextInt(chars.length)];
            sb.append(c);
        }
        return sb.toString();
    }

    @Test
    public void TestAppend() {
        VKmerList kmerList = new VKmerList();
        Assert.assertEquals(kmerList.size(), 0);
        //one kmer in list and reset each time
        VKmer kmer = new VKmer();
        kmer.setFromStringBytes(8, array, 0);
        kmerList.append(kmer);
        Assert.assertEquals("AATAGAAG", kmerList.getPosition(0).toString());
        Assert.assertEquals(1, kmerList.size());
        kmerList.clear();
        //add one more kmer each time and fix kmerSize
        for (int i = 0; i < 8; i++) {
            kmer.setFromStringBytes(i, array, 0);
            kmerList.append(kmer);
            Assert.assertEquals("AATAGAAG".substring(0, i), kmerList.getPosition(i).toString());
        }
        Assert.assertEquals(8, kmerList.size());

    }

    @Test
    public void TestRemove() {
        VKmerList kmerList = new VKmerList();
        Assert.assertEquals(kmerList.size(), 0);

        int i;
        VKmer kmer = new VKmer();
        for (i = 0; i < 200; i++) {
            kmer = new VKmer(5);
            String randomString = generaterRandomString(5);
            byte[] array = randomString.getBytes();
            kmer.setFromStringBytes(5, array, 0);
            kmerList.append(kmer);
            Assert.assertEquals(randomString, kmerList.getPosition(i).toString());
            Assert.assertEquals(i + 1, kmerList.size());
        }

        //delete one element each time
        VKmer tmpKmer = new VKmer(5);
        i = 0;
        VKmerList copyList = new VKmerList();
        copyList.setAsCopy(kmerList);
        Iterator<VKmer> iterator;
        for (int j = 0; j < 5; j++) {
            iterator = copyList.iterator();
            byte[] array = kmerList.getPosition(j).toString().getBytes();
            VKmer deletePos = new VKmer(5);
            deletePos.setFromStringBytes(5, array, 0);
            boolean removed = false;
            while (iterator.hasNext()) {
                tmpKmer = iterator.next();
                if (tmpKmer.equals(deletePos)) {
                    iterator.remove();
                    removed = true;
                    break;
                }
            }
            Assert.assertTrue(removed);
            Assert.assertEquals(200 - 1 - j, copyList.size());
            while (iterator.hasNext()) {
                tmpKmer = iterator.next();
                Assert.assertTrue(!tmpKmer.getBlockBytes().equals(deletePos.getBlockBytes()));
                i++;
            }
        }

        //delete all the elements
        i = 0;
        iterator = kmerList.iterator();
        while (iterator.hasNext()) {
            tmpKmer = iterator.next();
            iterator.remove();
        }
        Assert.assertEquals(0, kmerList.size());

        VKmerList edgeList = new VKmerList();
        VKmer k = new VKmer(3);
        k.setFromStringBytes(3, ("AAA").getBytes(), 0);
        edgeList.append(k);
        k.setFromStringBytes(3, ("CCC").getBytes(), 0);
        edgeList.append(k);
        Assert.assertEquals("AAA", edgeList.getPosition(0).toString());
        Assert.assertEquals("CCC", edgeList.getPosition(1).toString());
    }

    @Test
    public void simpleTestUnionUpdate() {
        VKmerList list1 = new VKmerList();
        VKmer a = new VKmer("AGCTAAATC");
        list1.append(a);
        VKmerList list2 = new VKmerList();
        VKmer b = new VKmer("AGCTAAATG");
        VKmer c = new VKmer("AGCTAAATC");
        list2.append(b);
        list2.append(c);
        list1.unionUpdate(list2);
        HashSet<VKmer> uniqueElements = new HashSet<VKmer>();
        uniqueElements.add(a);
        uniqueElements.add(b);
        uniqueElements.add(c);
        VKmerList expected = new VKmerList();

        ArrayList<String> arraylist1 = new ArrayList<String>();
        for (int i = 0; i < list1.size(); i++) {
            arraylist1.add(list1.getPosition(i).toString());
        }
        Collections.sort(arraylist1);

        for (VKmer kmer : uniqueElements) {
            expected.append(kmer);
        }
        ArrayList<String> arraylist2 = new ArrayList<String>();
        for (int i = 0; i < expected.size(); i++) {
            arraylist2.add(expected.getPosition(i).toString());
        }
        Collections.sort(arraylist2);

        Assert.assertEquals(arraylist1.size(), arraylist2.size());

        for (int i = 0; i < list1.size(); i++) {
            Assert.assertEquals(arraylist1.get(i), arraylist2.get(i));
        }
    }
}
