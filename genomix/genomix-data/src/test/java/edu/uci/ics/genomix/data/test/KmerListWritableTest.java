package edu.uci.ics.genomix.data.test;

import java.util.Iterator;
import java.util.Random;

import junit.framework.Assert;

import org.junit.Test;

import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.KmerListWritable;

public class KmerListWritableTest {

    @Test
    public void TestInitial() {
        KmerListWritable kmerList = new KmerListWritable();
        Assert.assertEquals(kmerList.getCountOfPosition(), 0);
        
        //one kmer in list and reset each time
        KmerBytesWritable kmer;
        for (int i = 1; i < 200; i++) {
            kmer = new KmerBytesWritable(i);
            String randomString = generateString(i);
            byte[] array = randomString.getBytes();
            kmer.setByRead(array, 0);
            kmerList.reset();
            kmerList.append(kmer);
            Assert.assertEquals(kmerList.getPosition(0).toString(), randomString);
            Assert.assertEquals(1, kmerList.getCountOfPosition());
        }
        
        kmerList.reset();
        //add one more kmer each time and fix kmerSize
        for (int i = 0; i < 200; i++) {
            kmer = new KmerBytesWritable(5);
            String randomString = generateString(5);
            byte[] array = randomString.getBytes();
            kmer.setByRead(array, 0);
            kmerList.append(kmer);
            Assert.assertEquals(kmerList.getPosition(i).toString(), randomString);
            Assert.assertEquals(i + 1, kmerList.getCountOfPosition());
        }
        
        byte [] another = new byte [kmerList.getLength()*2];
        int start = 20;
        System.arraycopy(kmerList.getByteArray(), 0, another, start, kmerList.getLength());
        KmerListWritable plist2 = new KmerListWritable(kmerList.kmerlength, kmerList.getCountOfPosition(),another,start);
        for(int i = 0; i < plist2.getCountOfPosition(); i++){
            Assert.assertEquals(kmerList.getPosition(i).toString(), plist2.getPosition(i).toString());
        }
    }
    
    @Test
    public void TestRemove() {
        KmerListWritable kmerList = new KmerListWritable();
        Assert.assertEquals(kmerList.getCountOfPosition(), 0);
        
        int i;
        KmerBytesWritable kmer;
        for (i = 0; i < 200; i++) {
            kmer = new KmerBytesWritable(5);
            String randomString = generateString(5);
            byte[] array = randomString.getBytes();
            kmer.setByRead(array, 0);
            kmerList.append(kmer);
            Assert.assertEquals(kmerList.getPosition(i).toString(), randomString);
            Assert.assertEquals(i + 1, kmerList.getCountOfPosition());
        }
        
        //delete one element each time
        KmerBytesWritable tmpKmer = new KmerBytesWritable();
        i = 0;
        KmerListWritable copyList = new KmerListWritable();
        copyList.set(kmerList);
        Iterator<KmerBytesWritable> iterator;
        for(int j = 0; j < 5; j++){
            iterator = copyList.iterator();
            byte[] array = kmerList.getPosition(j).toString().getBytes();
            KmerBytesWritable deletePos = new KmerBytesWritable(5);
            deletePos.setByRead(array, 0);
            while(iterator.hasNext()){
                tmpKmer = iterator.next();
                if(tmpKmer.equals(deletePos)){
                    iterator.remove();
                    break;
                }
            }
            Assert.assertEquals(200 - 1 - j, copyList.getCountOfPosition());
            while(iterator.hasNext()){
                tmpKmer = iterator.next();
                Assert.assertTrue(!tmpKmer.getBytes().equals(deletePos.getBytes()));
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
    }
    
    public String generateString(int n){
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
