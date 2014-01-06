package edu.uci.ics.genomix.driver.pipelinetests;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Assert;

import edu.uci.ics.genomix.data.types.Kmer;
import edu.uci.ics.genomix.data.types.Node;
import edu.uci.ics.genomix.data.types.ReadHeadInfo;
import edu.uci.ics.genomix.data.types.ReadHeadSet;
import edu.uci.ics.genomix.data.types.VKmer;
import edu.uci.ics.genomix.driver.GenomixDriver;

public class RandMergeOffsetTest {

    private static HashMap<Integer, Integer> expectedReadHeadOffset = new HashMap<Integer, Integer>();

    public enum KmerDir {
        FORWARD,
        REVERSE,
    }

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

    public static void buildExpectedResults(String expectedStr, String expectedFlipStr, boolean isFlipped,
            int kmerSize, int kmerNum) {
        KmerDir curKmerDir;
        Kmer.setGlobalKmerLength(kmerSize);
        Kmer curKmer = new Kmer();
        Kmer curReverseKmer = new Kmer();
        for (int i = 0; i < kmerNum - 1; i++) {
            String rawKmer = expectedStr.substring(i, i + kmerSize);
            curKmerDir = KmerDir.FORWARD;
            curKmer.setFromStringBytes(rawKmer.getBytes(), 0);
            curReverseKmer.setReversedFromStringBytes(rawKmer.getBytes(), 0);
            System.out.println(curKmer.toString());
            System.out.println(curReverseKmer.toString());
            curKmerDir = curKmer.compareTo(curReverseKmer) <= 0 ? KmerDir.FORWARD : KmerDir.REVERSE;
            switch (curKmerDir) {
                case FORWARD:
                    if (!isFlipped) {
                        int index = expectedStr.indexOf(curKmer.toString());
                        expectedReadHeadOffset.put(i, index);
                    } else {
                        int index = expectedFlipStr.indexOf(curReverseKmer.toString()) + kmerSize - 1;
                        expectedReadHeadOffset.put(i, index);
                    }
                    break;
                case REVERSE:
                    if (isFlipped) {
                        int index = expectedFlipStr.indexOf(curReverseKmer.toString());
                        expectedReadHeadOffset.put(i, index);
                    } else {
                        int index = expectedStr.indexOf(curKmer.toString()) + kmerSize - 1;
                        expectedReadHeadOffset.put(i, index);
                    }
                    break;
            }
        }
    }

    public static void readSequenceFileAndCompare(String inputGraph, String originStr, int kmerSize, int kmerNum) throws IOException {
        @SuppressWarnings("deprecation")
        JobConf conf = new JobConf();
        FileSystem dfs = FileSystem.getLocal(conf);
        SequenceFile.Reader reader = null;
        VKmer key = null;
        Node value = null;
        FileStatus[] files = dfs.globStatus(new Path(inputGraph + File.separator + "part*"));
        for (FileStatus f : files) {
            if (f.getLen() != 0) {
                try {
                    reader = new SequenceFile.Reader(dfs, f.getPath(), conf);
                    key = (VKmer) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
                    value = (Node) ReflectionUtils.newInstance(reader.getValueClass(), conf);
                    while (reader.next(key, value)) {
                        if (key == null || value == null)
                            throw new Exception("the results of key-value pair can not be null! ");
                        String actualMergeStr = value.getInternalKmer().toString();
                        String originFlipStr = getFlippedGeneStr(originStr);
                        if (originStr.equals(actualMergeStr)) {
                            buildExpectedResults(originStr, originFlipStr, false, kmerSize, kmerNum);
                        } else if (originFlipStr.equals(actualMergeStr)) {
                            buildExpectedResults(originStr, originFlipStr, true, kmerSize, kmerNum);
                        }
                        if (value.getFlippedReadIds().size() != 0) {
                            for (ReadHeadInfo iter : value.getFlippedReadIds().getOffSetRange(0, kmerNum)) {
                                if (expectedReadHeadOffset.containsKey((int) iter.getReadId())) {
                                    int expectedOffset = expectedReadHeadOffset.get((int) iter.getReadId());
                                    Assert.assertEquals(expectedOffset, iter.getOffset());
                                } else {
                                    Assert.fail();
                                }
                            }
                        }
                        if (value.getUnflippedReadIds().size() != 0) {
                            for (ReadHeadInfo iter : value.getUnflippedReadIds().getOffSetRange(0, kmerNum)) {
                                if (expectedReadHeadOffset.containsKey((int) iter.getReadId())) {
                                    int expectedOffset = expectedReadHeadOffset.get((int) iter.getReadId());
                                    Assert.assertEquals(expectedOffset, iter.getOffset());
                                } else {
                                    Assert.fail();
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    System.out.println("Test can not be passed! " + f + ":\n" + e);
                } finally {
                    if (reader != null)
                        reader.close();
                }
            }
        }
    }

    /**
     * before test, we need specify the kmerSize, kmerNum
     * To be noted that, we don't need to specify readLength (automatically already set it to kmerSize+1)
     * And the number of Reads is: kmerNum - 1
     * 
     * @param args
     * @return
     * @throws NumberFormatException
     * @throws Exception
     */
    public static String runPipeLine(String[] args, int kmerSize, int kmerNum) throws NumberFormatException, Exception {
        GenRandMultiReadForMergOffset test = new GenRandMultiReadForMergOffset(kmerSize, kmerNum);//kmerSize, kmerNum
        test.cleanDiskFile();
        String originStr = test.generateString();
        test.writeToDisk();
        String[] randmergePressureArgs = { "-runLocal", "-kmerLength", String.valueOf(5), "-readLengths",
                String.valueOf(6), "-saveIntermediateResults", "-localInput", test.getTestDir(), "-localOutput",
                "output", "-pipelineOrder", "BUILD_HYRACKS,MERGE" };
        GenomixDriver.main(randmergePressureArgs);
        test.cleanDiskFile();
        return originStr;
    }

    public static void main(String[] args) throws NumberFormatException, Exception {
        String originStr = runPipeLine(args, 5, 6);
        readSequenceFileAndCompare("output/FINAL-02-MERGE/bin", originStr, 5, 6);
        System.out.println("Test complete!");
    }
}
