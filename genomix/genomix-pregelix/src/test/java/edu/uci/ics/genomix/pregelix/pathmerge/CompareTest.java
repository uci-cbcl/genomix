package edu.uci.ics.genomix.pregelix.pathmerge;

import java.io.File;

import org.junit.Test;

import edu.uci.ics.genomix.pregelix.example.util.TestUtils;


public class CompareTest {
	public static final String PATH_TO_TESTSTORE = "testcase/pathmerge";
	public static final String CHAIN_OUTPUT = PATH_TO_TESTSTORE + "chain";
	
	@Test
	public void test() throws Exception {
		File naive = new File(CHAIN_OUTPUT + "/naive-sort");
		File log = new File(CHAIN_OUTPUT + "/log-sort");
		TestUtils.compareWithResult(naive, log);
	}
}
