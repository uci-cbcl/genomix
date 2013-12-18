package edu.uci.ics.genomix.driver.pipelinetests;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Assert;

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

    public static void buildExpectedResults(String expectedStr, String expectedFlipStr, boolean isFlipped) {
        KmerDir curKmerDir;
        VKmer curKmer = new VKmer();
        VKmer curReverseKmer = new VKmer();
        for (int i = 0; i < 5; i++) {
            String rawKmer = expectedStr.substring(i, i + 5);
            curKmerDir = KmerDir.FORWARD;
            curKmer.setFromStringBytes(rawKmer.getBytes(), 0);
            curReverseKmer.setReversedFromStringBytes(rawKmer.getBytes(), 0);
            curKmerDir = curKmer.compareTo(curReverseKmer) <= 0 ? KmerDir.FORWARD : KmerDir.REVERSE;
            switch (curKmerDir) {
                case FORWARD:
                    if (!isFlipped) {
                        int index = expectedStr.indexOf(curKmer.toString());
                        expectedReadHeadOffset.put(i, index);
                    } else {
                        int index = expectedFlipStr.indexOf(curReverseKmer.toString()) + 5;
                        expectedReadHeadOffset.put(i, index);
                    }
                    break;
                case REVERSE:
                    if (isFlipped) {
                        int index = expectedFlipStr.indexOf(curReverseKmer.toString());
                        expectedReadHeadOffset.put(i, index);
                    } else {
                        int index = expectedStr.indexOf(curKmer.toString()) + 5;
                        expectedReadHeadOffset.put(i, index);
                    }
                    break;
            }
        }

    }

    public static void readSequenceFileAndCompare(String inputGraph, String originStr) throws IOException {
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
                        //                        System.out.println(key.toString());
                        //                        System.out.println(value.toString());
                        String actualMergeStr = value.getInternalKmer().toString();
                        String originFlipStr = getFlippedGeneStr(originStr);
                        if (originStr.equals(actualMergeStr)) {
                            buildExpectedResults(originStr, originFlipStr, false);
                        } else if (originFlipStr.equals(actualMergeStr)) {
                            buildExpectedResults(originStr, originFlipStr, true);
                        }
                        if (value.getFlippedReadIds().size() != 0) {
                            for (ReadHeadInfo iter : value.getFlippedReadIds().getOffSetRange(0, 6)) {
                                if(expectedReadHeadOffset.containsKey(iter.getReadId())){
                                    int expectedOffset = expectedReadHeadOffset.get(iter.getReadId());
                                    Assert.assertEquals(expectedOffset, iter.getOffset());
                                }else{
                                    Assert.fail();
                                }
                            }
                        }
                        if (value.getUnflippedReadIds().size() != 0) {
                            for (ReadHeadInfo iter : value.getUnflippedReadIds().getOffSetRange(0, 6)) {
                                if(expectedReadHeadOffset.containsKey(iter.getReadId())){
                                    int expectedOffset = expectedReadHeadOffset.get(iter.getReadId());
                                    Assert.assertEquals(expectedOffset, iter.getOffset());
                                }else{
                                    Assert.fail();
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    System.out.println("Encountered an error getting stats for " + f + ":\n" + e);
                } finally {
                    if (reader != null)
                        reader.close();
                }
            }
        }
    }

    public static String runPipeLine(String[] args) throws NumberFormatException, Exception {
        GenRandMultiReadForMergOffset test = new GenRandMultiReadForMergOffset(5, 6);
        String originStr = test.generateString();
        test.writeToDisk();
        String[] randmergePressureArgs = { "-runLocal", "-kmerLength", String.valueOf(5), "-readLengths",
                String.valueOf(6), "-saveIntermediateResults", "-localInput", test.getTestDir(), "-localOutput",
                "output", "-pipelineOrder", "BUILD_HYRACKS,MERGE" };
        GenomixDriver.main(randmergePressureArgs);
        return originStr;
    }

    public static void main(String[] args) throws NumberFormatException, Exception {
        String originStr = runPipeLine(args);
        readSequenceFileAndCompare("output/FINAL-02-MERGE/bin", originStr);
        System.out.println("complete!");
    }
}
