package edu.uci.ics.genomix.pregelix.graph;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

public class Graph {
	
	/**
    * Construct a DOT graph in memory, convert it
    * to image and store the image in the file system.
	 * @throws Exception 
    */
   private void start(String fileName) throws Exception
   {
		File filePathTo = new File("graph/" + fileName);
		BufferedReader br = new BufferedReader(new FileReader(filePathTo));
		String line = "";
		String[] split;
		
		String precursor = "";
		String[] adjMap;
		char[] succeeds;
		String succeed = "";
		String output;
		
		GraphViz gv = new GraphViz();
		gv.addln(gv.start_graph());
		while((line = br.readLine()) != null){
			split = line.split("\t");
			precursor = split[0];
			adjMap = split[1].split("\\|"); 
			if(adjMap.length > 1){
				succeeds = adjMap[1].toCharArray();
				for(int i = 0; i < succeeds.length; i++){
					succeed = precursor.substring(1) + succeeds[i]; 
					output = precursor + " -> " + succeed;
					gv.addln(output);
				}
			}
		}
		gv.addln(gv.end_graph());
		System.out.println(gv.getDotSource());

		String type = "ps";
		File out = new File("graph/" + fileName + "_out." + type); // Linux
		gv.writeGraphToFile(gv.getGraph(gv.getDotSource(), type), out);
   }
	   
	public static void main(String[] args) throws Exception
	{
		Graph g = new Graph();
		g.start("BridgePath_7");
		g.start("CyclePath_7");
		g.start("SimplePath_7");
		g.start("SinglePath_7");
		g.start("TreePath_7");
	}
}
