/*
 * Copyright 2009-2013 by The Regents of the University of California
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

package edu.uci.ics.genomix.hyracks.graph.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;

public class TestUtils {
    /**
     * Compare with the sorted expected file.
     * The actual file may not be sorted;
     * 
     * @param expectedFile
     * @param actualFile
     */
    @SuppressWarnings("finally")
    public static boolean compareWithSortedResult(File expectedFile, File actualFile) throws Exception {
        BufferedReader readerActual = new BufferedReader(new FileReader(actualFile));
        BufferedReader readerExpected = new BufferedReader(new FileReader(expectedFile));
        ArrayList<String> actualLines = new ArrayList<String>();
        ArrayList<String> expectedLines = new ArrayList<String>();

        String lineExpected, lineActual;
        boolean flag = true;
        try {
            while ((lineActual = readerActual.readLine()) != null) {
                actualLines.add(lineActual);
            }
            Collections.sort(actualLines);

            while ((lineExpected = readerExpected.readLine()) != null) {
                expectedLines.add(lineExpected);
            }
            Collections.sort(expectedLines);

            int num = 0;
            for (String actualLine : actualLines) {
                lineExpected = expectedLines.get(num);
                if (lineExpected == null) {
                    flag = false;
                }
//                System.out.println(actualLine);
//                System.out.println(lineExpected);
                if (!equalStrings(lineExpected, actualLine)) {
                    flag = false;
                    System.out.println(lineExpected);
                    System.out.println(actualLine);
                }
                ++num;
            }
            lineExpected = expectedLines.get(num);
            if (lineExpected != null) {
                flag = false;
            }
        } finally {
            readerActual.close();
            readerExpected.close();
            return flag;
        }
    }

    public static void compareWithUnSortedPosition(File expectedFile, File actualFile, int[] poslistField)
            throws Exception {
        BufferedReader readerActual = new BufferedReader(new FileReader(actualFile));
        BufferedReader readerExpected = new BufferedReader(new FileReader(expectedFile));
        ArrayList<String> actualLines = new ArrayList<String>();
        String lineExpected, lineActual;
        try {
            while ((lineActual = readerActual.readLine()) != null) {
                actualLines.add(lineActual);
            }
            Collections.sort(actualLines);
            int num = 1;
            for (String actualLine : actualLines) {
                lineExpected = readerExpected.readLine();
                if (lineExpected == null) {
                    throw new Exception("Actual result changed at line " + num + ":\n< " + actualLine + "\n> ");
                }
                if (!containStrings(lineExpected, actualLine, poslistField)) {
                    throw new Exception("Result for changed at line " + num + ":\n< " + lineExpected + "\n> "
                            + actualLine);
                }
                ++num;
            }
            lineExpected = readerExpected.readLine();
            if (lineExpected != null) {
                throw new Exception("Actual result changed at line " + num + ":\n< \n> " + lineExpected);
            }
        } finally {
            readerActual.close();
            readerExpected.close();
        }
    }

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
                if (lineExpected.equalsIgnoreCase(lineActual)) {
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
        String[] rowsOne = s1.split("\n");
        String[] rowsTwo = s2.split("\n");

        if (rowsOne.length != rowsTwo.length)
            return false;

        for (int i = 0; i < rowsOne.length; i++) {
            String row1 = rowsOne[i];
            String row2 = rowsTwo[i];

            if (row1.equals(row2))
                continue;

            String[] fields1 = row1.split(",");
            String[] fields2 = row2.split(",");

            for (int j = 0; j < fields1.length; j++) {
                if (fields1[j].equals(fields2[j])) {
                    continue;
                } else if (fields1[j].indexOf('.') < 0) {
                    return false;
                } else {
                    fields1[j] = fields1[j].split("=")[1];
                    fields2[j] = fields2[j].split("=")[1];
                    Double double1 = Double.parseDouble(fields1[j]);
                    Double double2 = Double.parseDouble(fields2[j]);
                    float float1 = (float) double1.doubleValue();
                    float float2 = (float) double2.doubleValue();

                    if (Math.abs(float1 - float2) == 0)
                        continue;
                    else {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    private static boolean containStrings(String lineExpected, String actualLine, int[] poslistField) {
        if (lineExpected.equals(actualLine)) {
            return true;
        }
        String[] fieldsExp = lineExpected.split("\\\t");
        String[] fieldsAct = actualLine.split("\\\t");
        if (fieldsAct.length != fieldsExp.length) {
            return false;
        }
        for (int i = 0; i < fieldsAct.length; i++) {
            boolean cont = false;
            for (int x : poslistField) {
                if (i == x) {
                    cont = true;
                    break;
                }
            }
            if (cont) {
                continue;
            }
            if (!fieldsAct[i].equals(fieldsExp[i])) {
                return false;
            }
        }

        ArrayList<String> posExp = new ArrayList<String>();
        ArrayList<String> posAct = new ArrayList<String>();

        for (int x : poslistField) {
            String valueExp = lineExpected.split("\\\t")[x];
            for (int i = 1; i < valueExp.length() - 1;) {
                if (valueExp.charAt(i) == '(') {
                    String str = "";
                    i++;
                    while (i < valueExp.length() - 1 && valueExp.charAt(i) != ')') {
                        str += valueExp.charAt(i);
                        i++;
                    }
                    posExp.add(str);
                }
                i++;
            }
            String valueAct = actualLine.split("\\\t")[x];
            for (int i = 1; i < valueAct.length() - 1;) {
                if (valueAct.charAt(i) == '(') {
                    String str = "";
                    i++;
                    while (i < valueAct.length() - 1 && valueAct.charAt(i) != ')') {
                        str += valueAct.charAt(i);
                        i++;
                    }
                    posAct.add(str);
                }
                i++;
            }

            if (posExp.size() != posAct.size()) {
                return false;
            }
            Collections.sort(posExp);
            Collections.sort(posAct);
            for (int i = 0; i < posExp.size(); i++) {
                if (!posExp.get(i).equals(posAct.get(i))) {
                    return false;
                }
            }
        }
        return true;
    }
}
