package edu.uci.ics.genomix.pregelix.testcase;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

public class GenerateTestInput {

    /**
     * Simple Path
     */
    public static String simplePath(int kmerSize, int readLength, int numLines) {
        RandomString rs = new RandomString(kmerSize, readLength);
        String output = "";
        for (int i = 0; i < numLines; i++)
            output += rs.nextString(0) + "\r\n";
        return output;
        
    }

    /**
     * Bridge Path
     */
    public static String bridgePath(int kmerSize, int headLength, int bridgeLength, int readLength) {
        RandomString rs = new RandomString(kmerSize, readLength);
        String s1 = rs.nextString(0);
        rs.setLength(headLength + bridgeLength);
        int startBridge = kmerSize + headLength;
        rs.addString(s1.substring(0, startBridge));
        String s2 = rs.nextString(startBridge) + s1.substring(bridgeLength + startBridge - kmerSize);
        return s1 + "\r\n" + s2;
    }
    
    /**
     * Tree Path
     */
    public static String treePath(int kmerSize, int x, int y, int z) {
        RandomString rs = new RandomString(kmerSize, x + y + kmerSize - 1);
        String s1 = rs.nextString(0);
        rs.setLength(x + y + z + kmerSize - 1);
        rs.addString(s1.substring(0, x));
        String s2 = rs.nextString(x);
        rs.setLength(x + y + z + kmerSize - 1);
        rs.addString(s2.substring(0, x + y));
        String s3 = rs.nextString(x + y);
        return s1 + "\r\n" + s2 + "\r\n" + s3;
    }

    /**
     * Cycle Path
     */
    public static String cyclePath(int kmerSize, int length) {
        RandomString rs = new RandomString(kmerSize, length);
        String s1 = rs.nextString(0);
        String s2 = s1 + s1.substring(1, kmerSize + 1);
        return s2;
    }
    
    /**
     * Grid
     */
    public static String gridPath(int kmerSize){
        int length = kmerSize + kmerSize + kmerSize + 1;
        RandomString rs = new RandomString(kmerSize, length); // 3 + 3 + 3
        String row1 = rs.nextString(0, length).substring(0, length - 1);
        rs.nextString(kmerSize, kmerSize + 1);
        String row2 = rs.nextString(2*kmerSize, 2*kmerSize + 1).substring(1, length);
        String column1 = row1.substring(0, kmerSize) + row2.substring(0, 2*kmerSize - 1);
        String column2 = row1.substring(1, 1 + 2*kmerSize - 1) + row2.substring(2*kmerSize - 1, 3*kmerSize - 1);
        String column3 = row1.substring(1 + kmerSize, length - 1) + row2.substring(length -1);
        return row1 + "\r\n" + row2 + "\r\n" + column1 + "\r\n" + column2 + "\r\n" + column3;

    }
    
    public static void generateSimplePath(String destDir, int kmerSize, int readLength, int numLines){
        OutputStreamWriter writer;
        try {
            writer = new OutputStreamWriter(new FileOutputStream(destDir));
            writer.write(simplePath(kmerSize, readLength, numLines));
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public static void generateBridgePath(String destDir, int kmerSize, int headLength, int bridgeLength, int readLength){
        OutputStreamWriter writer;
        try {
            writer = new OutputStreamWriter(new FileOutputStream(destDir));
            writer.write(bridgePath(kmerSize, headLength, bridgeLength, readLength));
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public static void generateTreePath(String destDir, int kmerSize, int x, int y, int z){
        OutputStreamWriter writer;
        try {
            writer = new OutputStreamWriter(new FileOutputStream(destDir));
            writer.write(treePath(kmerSize, x, y, z));
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public static void generateCyclePath(String destDir, int kmerSize, int cycleLength){
        OutputStreamWriter writer;
        try {
            writer = new OutputStreamWriter(new FileOutputStream(destDir));
            writer.write(cyclePath(kmerSize, cycleLength));
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public static void generateGridPath(String destDir, int kmerSize){
        OutputStreamWriter writer;
        try {
            writer = new OutputStreamWriter(new FileOutputStream(destDir));
            writer.write(gridPath(kmerSize));
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public static void main(String[] args) {
        int kmerSize = 5; 
//        /** SimplePath **/
//        int readLength = 9;
//        int numLines = 3;
//        generateSimplePath("graph/SimplePath", kmerSize, readLength, numLines);
//        /** BridgePath **/
//        int headLength = 2;
//        int bridgeLength = 4; 
//        generateBridgePath("graph/BridgePath", kmerSize, headLength, bridgeLength, readLength);
//        /** TreePath **/
//        int x = 5;
//        int y = 5;
//        int z = 5;
//        generateTreePath("graph/TreePath", kmerSize, x, y, z);
//        /** CyclePath **/
//        int cycleLength = 8;
//        generateCyclePath("graph/CyclePath", kmerSize, cycleLength);
        /** GridPath **/
        generateGridPath("graph/GridPath", kmerSize);
    }
}
