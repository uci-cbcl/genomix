package edu.uci.ics.genomix.data.test;

import java.util.Iterator;
import java.util.Random;

import junit.framework.Assert;

import org.junit.Test;

import edu.uci.ics.genomix.type.VKmer;
import edu.uci.ics.genomix.type.VKmerList;

public class VKmerListTest {

    @Test
    public void TestInitial() {
        VKmerList kmerList = new VKmerList();
        Assert.assertEquals(kmerList.size(), 0);

        //one kmer in list and reset each time
        VKmer kmer;
        for (int i = 1; i < 200; i++) {
            kmer = new VKmer(i);
            String randomString = generaterRandomString(i);
            byte[] array = randomString.getBytes();
            kmer.setFromStringBytes(i, array, 0);
            kmerList.reset();
            kmerList.append(kmer);
            Assert.assertEquals(randomString, kmerList.getPosition(0).toString());
            Assert.assertEquals(1, kmerList.size());
        }

        kmerList.reset();
        //add one more kmer each time and fix kmerSize
        for (int i = 0; i < 200; i++) {
            kmer = new VKmer(5);
            String randomString = generaterRandomString(5);
            byte[] array = randomString.getBytes();
            kmer.setFromStringBytes(5, array, 0);
            kmerList.append(kmer);
            Assert.assertEquals(kmerList.getPosition(i).toString(), randomString);
            Assert.assertEquals(i + 1, kmerList.size());
        }

        byte[] another = new byte[kmerList.getLength() * 2];
        int start = 20;
        System.arraycopy(kmerList.getByteArray(), kmerList.getStartOffset(), another, start, kmerList.getLength());
        VKmerList plist2 = new VKmerList(another, start);
        for (int i = 0; i < plist2.size(); i++) {
            Assert.assertEquals(kmerList.getPosition(i).toString(), plist2.getPosition(i).toString());
        }
    }

    @Test
    public void TestRemove() {
        VKmerList kmerList = new VKmerList();
        Assert.assertEquals(kmerList.size(), 0);

        int i;
        VKmer kmer;
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
        copyList.setCopy(kmerList);
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

    public String generaterRandomString(int n) {
        char[] chars = "ACGT".toCharArray();
        StringBuilder sb = new StringBuilder();
        Random random = new Random();
        for (int i = 0; i < n; i++) {
            char c = chars[random.nextInt(chars.length)];
            sb.append(c);
        }
        return sb.toString();
    }
}
