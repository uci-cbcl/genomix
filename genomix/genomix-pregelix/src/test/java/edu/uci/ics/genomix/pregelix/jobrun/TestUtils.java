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

package edu.uci.ics.genomix.pregelix.jobrun;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Set;

public class TestUtils {

    /**
     * Compare the two files line by line.
     * 
     * @param expectedFile
     * @param actualFile
     * @throws Exception
     */
    public static void compareFiles(File expectedFile, File actualFile) throws Exception {
        compareFilesWithUnOrderedFields(expectedFile, actualFile, false, null);
    }

    /**
     * Compare with two files line by line.
     * This function will sort the lines inside both file by default String.compare() and then compare if every line is the same.
     * 
     * @param expectedFile
     * @param actualFile
     * @throws Exception
     */
    public static void compareFilesBySortingThemLineByLine(File expectedFile, File actualFile) throws Exception {
        compareFilesWithUnOrderedFields(expectedFile, actualFile, true, null);
    }

    /**
     * Compare two files line by line. Each line is composed by a few fields that was separated by "\t".
     * And it can contain some unordered fields which are separated by space (" "),
     * The unordered fields will be split by space and sorted later to compare with the expected answer.
     * This test utility is useful while we have a result file contains a merged line,
     * which could be generated in any order due to the multiple thread.
     * 
     * @param expectedFile
     * @param actualFile
     * @param tobeSorted
     *            : specify this compare need to be sorted or not.
     * @param unorderedField
     *            : specify the unordered fields list.
     * @throws Exception
     */
    public static void compareFilesWithUnOrderedFields(File expectedFile, File actualFile, boolean tobeSorted,
            final Set<Integer> unorderedField) throws Exception {
        ArrayList<String> actualLines = new ArrayList<String>();
        ArrayList<String> expectedLines = new ArrayList<String>();
        BufferedReader readerActual = new BufferedReader(new FileReader(actualFile));
        BufferedReader readerExpected = new BufferedReader(new FileReader(expectedFile));

        Comparator<String> fieldCompartor = new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                if (unorderedField == null || unorderedField.size() == 0) {
                    return o1.compareTo(o2);
                }
                String[] parts1 = o1.split("\\t");
                String[] parts2 = o2.split("\\t");
                for (int i = 0; i < parts1.length; i++) {
                    if (!unorderedField.contains(i)) {
                        int compareResult = parts1[i].compareTo(parts2[i]);
                        if (compareResult != 0) {
                            return compareResult;
                        }
                    }
                }
                return 0;
            }
        };
        String lineExpected, lineActual;
        while ((lineActual = readerActual.readLine()) != null) {
            actualLines.add(lineActual);
        }
        if (tobeSorted) {
            Collections.sort(actualLines, fieldCompartor);
        }

        while ((lineExpected = readerExpected.readLine()) != null) {
            expectedLines.add(lineExpected);
        }
        if (tobeSorted) {
            Collections.sort(expectedLines, fieldCompartor);
        }

        try {
            int num = 0;
            for (String actualLine : actualLines) {
                lineExpected = expectedLines.get(num++);
                if (lineExpected == null) {
                    throw new Exception("Actual result changed at line " + num + ":\n< " + actualLine + "\n> ");
                }
                if (unorderedField == null || unorderedField.size() == 0) {
                    if (!lineExpected.equals(actualLine)) {
                        throw new Exception("Compare two files not equal, Actual:" + actualLine + " Expected:"
                                + lineExpected);
                    }
                } else {
                    if (!compareStringByFields(lineExpected, actualLine, unorderedField)) {
                        throw new Exception("Result for changed at line " + num + ":\n< " + lineExpected + "\n> "
                                + actualLine);
                    }
                }
            }
            if (num < expectedLines.size()) {
                throw new Exception("Compare two files not equal, expected file have more lines");
            }
        } finally {
            readerActual.close();
            readerExpected.close();
        }
    }

    private static boolean compareStringByFields(String lineExpected, String actualLine,
            final Set<Integer> unorderedField) {
        if (lineExpected.equals(actualLine)) {
            return true;
        }
        String[] fieldsExp = lineExpected.split("\\t");
        String[] fieldsAct = actualLine.split("\\t");
        if (fieldsAct.length != fieldsExp.length) {
            return false;
        }

        ArrayList<String> unorderedFieldsExp = new ArrayList<String>();
        ArrayList<String> unorderedFieldsAct = new ArrayList<String>();

        //Compare the ordered field first.
        for (int i = 0; i < fieldsAct.length; i++) {
            if (unorderedField.contains(i)) {
                unorderedFieldsExp.add(fieldsExp[i]);
                unorderedFieldsAct.add(fieldsAct[i]);
            } else {
                if (!fieldsAct[i].equals(fieldsExp[i])) {
                    return false;
                }
            }
        }

        for (int i = 0; i < unorderedFieldsAct.size(); i++) {
            ArrayList<String> subfieldsAct = new ArrayList<String>(Arrays.asList(unorderedFieldsAct.get(i)
                    .split("\\s+")));
            ArrayList<String> subfieldsExp = new ArrayList<String>(Arrays.asList(unorderedFieldsExp.get(i)
                    .split("\\s+")));

            Collections.sort(subfieldsAct);
            Collections.sort(subfieldsExp);

            //Compare the unordered fields
            for (int j = 0; j < unorderedFieldsExp.size(); j++) {
                if (!unorderedFieldsExp.get(j).equals(unorderedFieldsAct.get(j))) {
                    return false;
                }
            }
        }
        return true;
    }
}
