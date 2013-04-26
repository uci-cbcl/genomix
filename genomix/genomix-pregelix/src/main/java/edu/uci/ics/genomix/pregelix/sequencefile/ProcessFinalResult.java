package edu.uci.ics.genomix.pregelix.sequencefile;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.text.DecimalFormat;

public class ProcessFinalResult {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		DecimalFormat df = new DecimalFormat("0.00");
		BufferedReader br = new BufferedReader(new FileReader("log2_unfinite"));
		BufferedWriter bw = new BufferedWriter(new FileWriter("report3"));
		String line;
		int i = 0;
		double totalTime = 0;
		while((line = br.readLine()) != null){
			String[] tokens = line.split(" ");
			for(i = 0; i < tokens.length - 1; i++){
				bw.write(tokens[i] + " ");
			}
			String subString = tokens[i].substring(0, tokens[i].length() - 2);
			double ms = Double.parseDouble(subString)/60000;
			totalTime += ms;
			bw.write(df.format(ms) + "m");
			bw.newLine();
		}
		bw.write("The total time is " + df.format(totalTime) + "m");
		bw.close();
		br.close();
	}

}
