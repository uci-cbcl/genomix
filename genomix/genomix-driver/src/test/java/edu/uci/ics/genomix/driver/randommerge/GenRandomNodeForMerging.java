package edu.uci.ics.genomix.driver.randommerge;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.logging.Logger;
import org.apache.commons.io.FileUtils;
import edu.uci.ics.genomix.data.types.Kmer;
import edu.uci.ics.genomix.hyracks.graph.driver.GenomixHyracksDriver;

public class GenRandomNodeForMerging {

    private static final char[] symbols = new char[4];
    private static final Logger LOG = Logger.getLogger(GenomixHyracksDriver.class.getName());

    static {
        symbols[0] = 'A';
        symbols[1] = 'C';
        symbols[2] = 'G';
        symbols[3] = 'T';
    }

    public enum KmerDir {
        FORWARD,
        REVERSE,
    }

    private final Random random = new Random();
    private char[] buf;
    private HashSet<Kmer> nonDupSet;
    private int k;
    private int kmerNum;
    private Kmer tempKmer;
    private Kmer tempReverseKmer;
    private KmerDir curKmerDir = KmerDir.FORWARD;
    private String targetPath;

    public GenRandomNodeForMerging(int kmerSize, int kmerNum) {
        if (kmerNum < 1)
            throw new IllegalArgumentException("length < 1: " + kmerNum);
        buf = new char[kmerSize + kmerNum - 1];
        this.k = kmerSize;
        this.kmerNum = kmerNum;
        this.nonDupSet = new HashSet<Kmer>(kmerNum);
        Kmer.setGlobalKmerLength(kmerSize);
        tempKmer = new Kmer();
        tempReverseKmer = new Kmer();
        targetPath = "randommerge" + File.separator + "randommergeseq.fastq";
    }

    public void generateString() {
        String tmp = "";
        int count = 4;
        LOG.info("Begin to generate string !");
        for (int idx = 0; idx < buf.length;) {
            buf[idx] = symbols[random.nextInt(4)];
            if (idx >= k - 1) {
                tmp = new String(buf, idx - k + 1, k);
                tempKmer.setFromStringBytes(tmp.getBytes(), 0);
                tempReverseKmer.setReversedFromStringBytes(tmp.getBytes(), 0);
                curKmerDir = tempKmer.compareTo(tempReverseKmer) <= 0 ? KmerDir.FORWARD : KmerDir.REVERSE;
                switch (curKmerDir) {
                    case FORWARD:
                        if (!nonDupSet.contains(tempKmer)) {
                            nonDupSet.add(new Kmer(tempKmer));
                            idx++;
                            count = 4;
                        } else if (count == 0) {
                            idx++;
                            count = 4;
                            LOG.info("there must be a duplicate in read! " + idx);
                        } else
                            count--;
                        break;
                    case REVERSE:
                        if (!nonDupSet.contains(tempReverseKmer)) {
                            nonDupSet.add(new Kmer(tempReverseKmer));
                            idx++;
                            count = 4;
                        } else if (count == 0) {
                            idx++;
                            count = 4;
                        } else
                            count--;
                        break;
                }
            } else
                idx++;
        }
        LOG.info("End to generate string !");
    }

    public void writeToDisk() throws IOException {
        System.out.println("-----------------------");
        System.out.println("the original sequence is:" + String.valueOf(buf));
        String[] reads = new String[this.kmerNum - 1];
        for (int i = 0; i < this.kmerNum - 1; i++) {
            reads[i] = new String(buf, i, k + 1);
        }
        BufferedWriter writer = null;
        writer = new BufferedWriter(new FileWriter(targetPath));
        System.out.println("-----------------------");
        for (int i = 0; i < reads.length; i++) {
            writer.write(i + "\t" + reads[i]);
            System.out.println("read: " + i + "\t" + reads[i]);
            writer.newLine();
        }
        System.out.println("-----------------------");
        writer.close();
    }

    public void cleanDiskFile() throws IOException, InterruptedException {
        File targetFile = new File(targetPath);
        if (targetFile.getParentFile().exists()) {
            String cleanFile = "rm -r -f " + targetFile.getParent();
            Process p = Runtime.getRuntime().exec(cleanFile);
            p.waitFor();
            if (p.exitValue() != 0)
                throw new RuntimeException("Failed to delete the path" + targetFile.getParent());
        }
    }

    public String getTestDir() {
        return targetPath;
    }
}
