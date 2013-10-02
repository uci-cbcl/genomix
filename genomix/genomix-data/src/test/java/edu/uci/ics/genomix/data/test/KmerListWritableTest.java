package edu.uci.ics.genomix.data.test;

import java.util.Iterator;
import java.util.Random;

import junit.framework.Assert;

import org.junit.Test;

import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerListWritable;

public class KmerListWritableTest {

    @Test
    public void TestInitial() {
        VKmerListWritable kmerList = new VKmerListWritable();
        Assert.assertEquals(kmerList.getCountOfPosition(), 0);
        
        //one kmer in list and reset each time
        VKmerBytesWritable kmer;
        for (int i = 1; i < 200; i++) {
            kmer = new VKmerBytesWritable(i);
            String randomString = generaterRandomString(i);
            byte[] array = randomString.getBytes();
            kmer.setFromStringBytes(i, array, 0);
            kmerList.reset();
            kmerList.append(kmer);
            Assert.assertEquals(randomString, kmerList.getPosition(0).toString());
            Assert.assertEquals(1, kmerList.getCountOfPosition());
        }
        
        kmerList.reset();
        //add one more kmer each time and fix kmerSize
        for (int i = 0; i < 200; i++) {
            kmer = new VKmerBytesWritable(5);
            String randomString = generaterRandomString(5);
            byte[] array = randomString.getBytes();
            kmer.setFromStringBytes(5, array, 0);
            kmerList.append(kmer);
            Assert.assertEquals(kmerList.getPosition(i).toString(), randomString);
            Assert.assertEquals(i + 1, kmerList.getCountOfPosition());
        }
        
        byte [] another = new byte [kmerList.getLength()*2];
        int start = 20;
        System.arraycopy(kmerList.getByteArray(), kmerList.getStartOffset(), another, start, kmerList.getLength());
        VKmerListWritable plist2 = new VKmerListWritable(another, start);
        for(int i = 0; i < plist2.getCountOfPosition(); i++){
            Assert.assertEquals(kmerList.getPosition(i).toString(), plist2.getPosition(i).toString());
        }
    }
    
    @Test
    public void TestRemove() {
        VKmerListWritable kmerList = new VKmerListWritable();
        Assert.assertEquals(kmerList.getCountOfPosition(), 0);
        
        int i;
        VKmerBytesWritable kmer;
        for (i = 0; i < 200; i++) {
            kmer = new VKmerBytesWritable(5);
            String randomString = generaterRandomString(5);
            byte[] array = randomString.getBytes();
            kmer.setFromStringBytes(5, array, 0);
            kmerList.append(kmer);
            Assert.assertEquals(randomString, kmerList.getPosition(i).toString());
            Assert.assertEquals(i + 1, kmerList.getCountOfPosition());
        }
        
        //delete one element each time
        VKmerBytesWritable tmpKmer = new VKmerBytesWritable(5);
        i = 0;
        VKmerListWritable copyList = new VKmerListWritable();
        copyList.setCopy(kmerList);
        Iterator<VKmerBytesWritable> iterator;
        for(int j = 0; j < 5; j++){
            iterator = copyList.iterator();
            byte[] array = kmerList.getPosition(j).toString().getBytes();
            VKmerBytesWritable deletePos = new VKmerBytesWritable(5);
            deletePos.setFromStringBytes(5, array, 0);
            boolean removed = false;
            while(iterator.hasNext()){
                tmpKmer = iterator.next();
                if(tmpKmer.equals(deletePos)){
                    iterator.remove();
                    removed = true;
                    break;
                }
            }
            Assert.assertTrue(removed);
            Assert.assertEquals(200 - 1 - j, copyList.getCountOfPosition());
            while(iterator.hasNext()){
                tmpKmer = iterator.next();
                Assert.assertTrue(!tmpKmer.getBlockBytes().equals(deletePos.getBlockBytes()));
                i++;
            }
        }
        
        //delete all the elements
        i = 0;
        iterator = kmerList.iterator();
        while(iterator.hasNext()){
            tmpKmer = iterator.next();
            iterator.remove();
        }
        
        Assert.assertEquals(0, kmerList.getCountOfPosition());
        
        VKmerListWritable edgeList = new VKmerListWritable();
        VKmerBytesWritable k = new VKmerBytesWritable(3);
        k.setFromStringBytes(3, ("AAA").getBytes(), 0);
        edgeList.append(k);
        k.setFromStringBytes(3, ("CCC").getBytes(), 0);
        edgeList.append(k);
        for(VKmerBytesWritable edge : edgeList){
        	System.out.println(edge.toString());
        }
    }
    
    public String generaterRandomString(int n){
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
