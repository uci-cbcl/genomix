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
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;

public class VKmerListRandomTest {

    public static int strMaxLength;
    public static int strMinLength;
    public static int maxLoopNum;
    public static int minLoopNum;

    @Before
    public void setUp() {
        strMaxLength = 15;
        strMinLength = 11;
        minLoopNum = 20;
        maxLoopNum = 50;
        if ((strMinLength <= 0) || (strMaxLength <= 0)) {
            throw new IllegalArgumentException("strMinLength or strMaxLength can not be less than 0!");
        }
        if (strMinLength > strMaxLength) {
            throw new IllegalArgumentException("strMinLength can not be larger than strMaxLength!");
        }
        if ((maxLoopNum <= 0) || (minLoopNum <= 0)) {
            throw new IllegalArgumentException("maxLoopNum or minLoopNum can not be less than 0!");
        }
        if (minLoopNum > maxLoopNum) {
            throw new IllegalArgumentException("minLoopNum can not be larger than maxLoopNum");
        }
    }

    public static VKmer getRandomVKmer(String input, int strLength) {
        int kmerSize = RandomTestHelper.genRandomInt(1, strLength);
        String actualKmerStr = input.substring(0, kmerSize);
        VKmer vkmer = new VKmer();
        vkmer.setFromStringBytes(actualKmerStr.getBytes(), 0);
        return vkmer;
    }

    @Test
    public void TestAppend() {
        VKmerList kmerList = new VKmerList();
        Assert.assertEquals(kmerList.size(), 0);
        kmerList.clear();
        int loop = RandomTestHelper.genRandomInt(minLoopNum, maxLoopNum);
        for (int i = 0; i < loop; i++) {
            int strLength = RandomTestHelper.genRandomInt(strMinLength, strMaxLength);
            String input = RandomTestHelper.generateGeneString(strLength);
            VKmer vkmer = getRandomVKmer(input, strLength);
            kmerList.append(vkmer);
            Assert.assertEquals(input.substring(0, vkmer.getKmerLetterLength()), kmerList.getPosition(i).toString());
        }
        Assert.assertEquals(loop, kmerList.size());
    }

    @Test
    public void TestRemove() {
        VKmerList kmerList = new VKmerList();
        Assert.assertEquals(kmerList.size(), 0);
        int loop = RandomTestHelper.genRandomInt(minLoopNum, maxLoopNum);
        ArrayList<String> expectedList = new ArrayList<String>();
        for (int i = 0; i < loop; i++) {
            int strLength = RandomTestHelper.genRandomInt(strMinLength, strMaxLength);
            String input = RandomTestHelper.generateGeneString(strLength);
            VKmer vkmer = getRandomVKmer(input, strLength);
            kmerList.append(vkmer);
            expectedList.add(vkmer.toString());
        }
        Assert.assertEquals(loop, kmerList.size());
        Iterator<VKmer> iterator;
        for (int i = 0; i < loop; i++) {
            iterator = kmerList.iterator();
            boolean removed = false;
            while (iterator.hasNext()) {
                VKmer tmpKmer = iterator.next();
                if (tmpKmer.toString().equals(expectedList.get(i))) {
                    iterator.remove();
                    expectedList.remove(i);
                    removed = true;
                    break;
                }
            }
            Assert.assertTrue(removed);
            for(int j = 0; j < expectedList.size(); i++) {
                Assert.assertEquals(expectedList.get(j), kmerList.getPosition(j).toString());
            }
        }
    }
    
    @Test
    public void complicatedTestUnionUpdate() {
        VKmerList kmerList1 = new VKmerList();
        VKmerList kmerList2 = new VKmerList();
        HashSet<VKmer> uniqueElements = new HashSet<VKmer>();
        int loop1 = RandomTestHelper.genRandomInt(minLoopNum, maxLoopNum);
        int loop2 = RandomTestHelper.genRandomInt(minLoopNum, maxLoopNum);
        for (int i = 1; i < loop1; i++) {
            int strLength = RandomTestHelper.genRandomInt(strMinLength, strMaxLength);
            String input = RandomTestHelper.generateGeneString(strLength);
            VKmer vkmer = getRandomVKmer(input, strLength);
            kmerList1.append(vkmer);
            uniqueElements.add(new VKmer(vkmer));
        }
        for (int i = 1; i < loop2; i++) {
            int strLength = RandomTestHelper.genRandomInt(strMinLength, strMaxLength);
            String input = RandomTestHelper.generateGeneString(strLength);
            VKmer vkmer = getRandomVKmer(input, strLength);
            kmerList1.append(vkmer);
            uniqueElements.add(new VKmer(vkmer));
        }
        kmerList1.unionUpdate(kmerList2);
        ArrayList<String> list1 = new ArrayList<String>();
        for (int i = 0; i < kmerList1.size(); i++) {
            list1.add(kmerList1.getPosition(i).toString());
        }
        Collections.sort(list1);
        VKmerList expected = new VKmerList();
        for (VKmer iter : uniqueElements) {
            expected.append(iter);
        }
        ArrayList<String> list2 = new ArrayList<String>();
        for (int i = 0; i < expected.size(); i++) {
            list2.add(expected.getPosition(i).toString());
        }
        Collections.sort(list2);
        Assert.assertEquals(list1.size(), list2.size());
        for (int i = 0; i < list1.size(); i++) {
            Assert.assertEquals(list1.get(i), list2.get(i));
        }
    }

}
