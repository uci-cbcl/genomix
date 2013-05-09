package edu.uci.ics.genomix.pregelix.ResultGen;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.text.DecimalFormat;

import org.apache.commons.io.FileUtils;

public class ReportGenerator {
	public static final String PATH_TO_REPORT = "report";
	public static final String PATH_TO_LOGINFO = "log";

	public static void generateReportFromLoginfo(String fileName) throws Exception {
		DecimalFormat df = new DecimalFormat("0.00");
		BufferedReader br = new BufferedReader(new FileReader(PATH_TO_LOGINFO + "/" + fileName));
		BufferedWriter bw = new BufferedWriter(new FileWriter(new File(PATH_TO_REPORT + "/" + fileName)));
		String line;
		int i = 0;
		double totalTime = 0;
		line = br.readLine();
		do{
			line = br.readLine();
			String[] tokens = line.split(" ");
			for(i = 1; i < tokens.length - 1; i++){
				bw.write(tokens[i] + " ");
			}
			String subString = tokens[i].substring(0, tokens[i].length() - 2);
			double ms = Double.parseDouble(subString)/60000;
			totalTime += ms;
			bw.write(df.format(ms) + "m");
			bw.newLine();
		}while((line = br.readLine()) != null);
		bw.write("The total time is " + df.format(totalTime) + "m");
		bw.close();
		br.close();
	}
	
	public static void main(String[] args) throws Exception {
		FileUtils.forceMkdir(new File(PATH_TO_REPORT));
		FileUtils.cleanDirectory(new File(PATH_TO_REPORT));
		generateReportFromLoginfo("naive_converge");
		generateReportFromLoginfo("log_converge");
		generateReportFromLoginfo("naive_36");
		generateReportFromLoginfo("log_13");

	}
}
