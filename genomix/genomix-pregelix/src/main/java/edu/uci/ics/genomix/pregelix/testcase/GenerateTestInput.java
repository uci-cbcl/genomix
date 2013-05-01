package edu.uci.ics.genomix.pregelix.testcase;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

public class GenerateTestInput {

	/**
	 * Simple Path
	 */
	public static String simplePath(int k, int length, int numLines){
		RandomString rs = new RandomString(k, length);
		String output = "";
		for(int i = 0; i < numLines; i++)
			output += rs.nextString(0) + "\r\n";
		return output;
	}
	/**
	 * Tree Path
	 */
	public static String treePath(int k, int x, int y, int z){
		RandomString rs = new RandomString(k, x + y + k - 1);
		String s1 = rs.nextString(0);
		rs.setLength(x + y + z + k - 1);
		rs.addString(s1.substring(0, x));
		String s2 = rs.nextString(x);
		rs.setLength(x + y + z + k - 1);
		rs.addString(s2.substring(0,x + y));
		String s3 = rs.nextString(x + y);
		return s1 + "\r\n" + s2 + "\r\n" + s3;
	}
	/**
	 * Cycle Path  
	 */
	public static String cyclePath(int k, int length){
		RandomString rs = new RandomString(k, length);
		String s1 = rs.nextString(0);
		String s2 = s1 + s1.substring(1, k + 1);
		return s2;
	}
	/**
	 * Bridge Path
	 */
	public static String bridgePath(int k, int x){
		RandomString rs = new RandomString(k, x + k + 2 + k - 1);
		String s1 = rs.nextString(0);
		rs.setLength(x + k + 2);
		rs.addString(s1.substring(0, k + 2));
		String s2 = rs.nextString(k + 2) + s1.substring(x + k + 2, x + k + 2 + k - 1);
		return s1 + "\r\n" + s2;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		OutputStreamWriter writer;
		try {
			writer = new OutputStreamWriter(new FileOutputStream("graph/55/SinglePath_55"));
			writer.write(simplePath(55,320,1));
			writer.close();
			/*writer = new OutputStreamWriter(new FileOutputStream("graph/55/SimplePath_55"));
			writer.write(simplePath(55,60,3));
			writer.close();
			writer = new OutputStreamWriter(new FileOutputStream("graph/55/TreePath"));
			writer.write(treePath(55, 5, 5, 3));
			writer.close();
			writer = new OutputStreamWriter(new FileOutputStream("graph/55/CyclePath"));
			writer.write(cyclePath(55,60));
			writer.close();
			writer = new OutputStreamWriter(new FileOutputStream("graph/55/BridgePath"));
			writer.write(bridgePath(55,2));
			writer.close();*/
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
