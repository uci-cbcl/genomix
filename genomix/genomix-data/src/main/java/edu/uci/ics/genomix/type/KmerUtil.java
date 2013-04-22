package edu.uci.ics.genomix.type;

public class KmerUtil {
    public static final String empty = "";

    public static int getByteNumFromK(int k) {
        int x = k / 4;
        if (k % 4 != 0) {
            x += 1;
        }
        return x;
    }

    public static byte reverseKmerByte(byte k) {
        int x = (((k >> 2) & 0x33) | ((k << 2) & 0xcc));
        return (byte) (((x >> 4) & 0x0f) | ((x << 4) & 0xf0));
    }

    public static String recoverKmerFrom(int k, byte[] keyData, int keyStart, int keyLength) {
        StringBuilder strKmer = new StringBuilder();
        int byteId = keyStart + keyLength - 1;
        if (byteId < 0){
            return empty;
        }
        byte currentbyte = keyData[byteId];
        for (int geneCount = 0; geneCount < k; geneCount++) {
            if (geneCount % 4 == 0 && geneCount > 0) {
                currentbyte = keyData[--byteId];
            }
            strKmer.append((char) GeneCode.GENE_SYMBOL[(currentbyte >> ((geneCount % 4) * 2)) & 0x03]);
        }
        return strKmer.toString();
    }

}
