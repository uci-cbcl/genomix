package edu.uci.ics.genomix.hyracks.graph.test;

import java.io.File;
import java.io.FilenameFilter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GraphBuildingTestSetting {

    public static final String TEST_FILE_EXTENSION = ".test.txt";

    public String[] parseArgumentFromFilename(String filename) {
        Pattern kPattern = Pattern.compile("_k(\\d+)");
        Matcher m = kPattern.matcher(filename);
        if (m.find()) {
            return new String[] { m.group(1) };
        }
        return null;
    }

    public FilenameFilter getFilenameFilter() {
        return new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.toLowerCase().endsWith(TEST_FILE_EXTENSION);
            }
        };
    }
}
