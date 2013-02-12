package edu.uci.ics.utils;

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
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

/**
 * This class offer the service for graphbuildingtest.class
 */
public class TestUtils {
    public static void compareWithResult(File expectedFile, File actualFile) throws Exception {
        BufferedReader readerExpected = new BufferedReader(new FileReader(expectedFile));
        BufferedReader readerActual = new BufferedReader(new FileReader(actualFile));
        String lineExpected, lineActual;
        int num = 1;
        try {
            while ((lineExpected = readerExpected.readLine()) != null) {
                lineActual = readerActual.readLine();
                // Assert.assertEquals(lineExpected, lineActual);
                if (lineActual == null) {
                    throw new Exception("Actual result changed at line " + num + ":\n< " + lineExpected + "\n> ");
                }
                if (!equalStrings(lineExpected, lineActual)) {
                    throw new Exception("Result for changed at line " + num + ":\n< " + lineExpected + "\n> "
                            + lineActual);
                }
                ++num;
            }
            lineActual = readerActual.readLine();
            if (lineActual != null) {
                throw new Exception("Actual result changed at line " + num + ":\n< \n> " + lineActual);
            }
        } finally {
            readerExpected.close();
            readerActual.close();
        }
    }

    private static boolean equalStrings(String s1, String s2) {
        String[] rowsOne = s1.split("\t");
        String[] rowsTwo = s2.split("\t");

        if (rowsOne.length != rowsTwo.length)
            return false;

        for (int i = 0; i < rowsOne.length; i++) {
            String row1 = rowsOne[i];
            String row2 = rowsTwo[i];

            if (row1.equals(row2))
                continue;
            else
                return false;
        }
        return true;
    }

    public static void main(String[] args) throws Exception {
        TestUtils TUtils = new TestUtils();
        TUtils.compareWithResult(new File("/Users/hadoop/Documents/workspace/Test/part-00000"), new File(
                "/Users/hadoop/Documents/workspace/Test/test.txt"));
    }
}
