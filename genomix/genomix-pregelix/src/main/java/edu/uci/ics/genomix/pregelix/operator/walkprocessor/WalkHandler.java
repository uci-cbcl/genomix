package edu.uci.ics.genomix.pregelix.operator.walkprocessor;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.FileUtils;

import edu.uci.ics.genomix.data.types.VKmer;
import edu.uci.ics.genomix.data.types.VKmerList;

public class WalkHandler {
	private static final int MIN_OVERLAPSIZE = 4;
	private static final int kmerSize = 21;
	private static HashMap <VKmer, VKmerList> walkIdMap = new HashMap<VKmer, VKmerList>();
	private static HashMap <VKmer, VKmer> walkKmerMap = new HashMap<VKmer, VKmer>();
	private static List<String> walkList = new ArrayList<String>();
	static PrintWriter writer;
	
    public static void writeOnFile() throws FileNotFoundException, UnsupportedEncodingException {
        String s = "/home/elmira/WORK/RESULTS/result.txt";
        writer = new PrintWriter(s, "UTF-8");
    }
	/**
	 * Reads all .txt file from a directory and build a hashmap for walks.
	 * @param directory
	 * @return
	 * @throws IOException
	 */
	public HashMap <VKmer, VKmerList> loadWalkMap(final File directory , String Id) throws IOException{
		VKmerList walk = new VKmerList();
		VKmer node = new VKmer();
		VKmer vkmer = new VKmer();
		for (final File file : directory.listFiles()) {
			walk.clear();
	        if (!file.isDirectory()) {
	        	String content = FileUtils.readFileToString(file);
	        	String [] parts = content.split("\n");
	        	String [] words = parts[0].split("[\\W]");
	        	for (String word : words){
	        		node.setAsCopy(word);
	        		walk.append(node);;
	        	}
	        	node.setAsCopy(file.getName());
	        	vkmer.setAsCopy(parts[1]);
	        	walkIdMap.put(node, walk);
	        	walkKmerMap.put(node, vkmer);
	        	walkList.add(vkmer.toString());
	        }
	    }
		
		return walkIdMap;
		
	}
	/**
	 * This method gives back the walk for each seed.
	 * However for now, there is no good implementation to store the walks.
	 * @param id
	 * @return
	 */
	public VKmerList loadWalk(VKmer id){
		return walkIdMap.get(id);
	}
	
	public VKmer loadAccWalk(VKmer id){
		return walkKmerMap.get(id);
	}
	
	/**
	 * We need to find the overlap between walks, and it doesn't include all types of overlap.
	 * I just considered few specific situation here.
	 * Need to think about a more efficient way.
	 * @param walk1
	 * @param walk2
	 * @return
	 */
	public ArrayList<VKmerList> compareWalk(VKmerList walk1, VKmerList walk2){
		ArrayList<VKmerList> walks = new ArrayList<VKmerList>(2);
		VKmerList walk = new VKmerList();
		int first = 0;
		if (walk1.contains(walk2.getPosition(walk2.size() - 1))){
			first = 1;
		}else if(walk2.contains(walk1.getPosition(walk1.size() - 1))){
			walk.setAsCopy(walk2);
			walk2.setAsCopy(walk1);
			walk1.setAsCopy(walk);
			first = 2;
		} else{
			walks.add(walk1);
			walks.add(walk2);
			return walks;
		}
		//Do the walks need a complete overlap?
		walk.clear();
		int overlapSize = 0;
		int counter = walk1.indexOf(walk2.getPosition(walk2.size() - 1));
		while (walk1.getPosition(counter) ==  walk2.getPosition(walk2.size()-1 - overlapSize) && counter > 0 && walk2.size() > overlapSize){
			overlapSize ++;
			counter--;
		} 
		/**
		 * Example:
		 *	walk1: --------------------------->
		 *	walk2:       ---------------->
		 *		or
		 *	walk1:  -------------------->
		 *	walk2:  -------------------->
		 *  The second walk is extra.
		 */
		if (overlapSize == walk2.size()){
			return keepWalkOrder(walk1, walk, walks, first);
		/**
		 * Example:
		 *	walk1: 			--------------------->
		 *	walk2:	---------------->
		 *	 send back the first walk and not overlapping part of the second walk
		 */
			
		/**
		* Example:
		*                     \
		* 					   \							
		*	walk1:			    ---------------->
		*	walk2:				--------------->
		*                     /
		*                    / 
		*	 send back the first walk and not overlapping part of the second walk
		*/
		}else if(counter == 0 || overlapSize > MIN_OVERLAPSIZE){
			return keepWalkOrder(walk1,walk2.subList(0, walk2.size()- overlapSize), walks, first);
		}
		
		return keepWalkOrder(walk1, walk2, walks, first);
		
		
		
	}
	
	/**
	 * This method aligns string walks. It considers the same cases as compareWalk.
	 * @param walk1
	 * @param walk2
	 * @return
	 */
	
	public static ArrayList<String> alignWalk(String walk1, String walk2){
		ArrayList<String> walks = new ArrayList<String>(2);
		String walk = new String();
		String overlap = new String();
		overlap = longestSubstring(walk1, walk2);
		int overlapSize = overlap.length();
		if (overlapSize > MIN_OVERLAPSIZE ){
			if (walk1.endsWith(overlap)){
					walks.add(walk1.substring(0, walk1.length() - overlapSize));
					walks.add(walk2);
					return walks;
			}else if (walk2.endsWith(overlap)){
					walks.add(walk1);
					walks.add(walk2.substring(0, walk2.length() - overlapSize));
					return walks;
			}else if (walk1.contains(walk2)){
				walks.add(walk1);
				walks.add(walk);
				return walks;
			}else if (walk2.contains(walk1)){
				walks.add(walk);
				walks.add(walk2);
				return walks;
			}
		}
		
		walks.add(walk1);
		walks.add(walk2);
		return walks;
	}

	public ArrayList<VKmerList> keepWalkOrder(VKmerList walk1,
			 VKmerList walk, ArrayList<VKmerList> walks, int first) {
		if (first == 1){
			walks.add(walk1);
			walks.add(walk);
			return walks;
		}
		else{
			walks.add(walk);
			walks.add(walk1);
			return walks;
		}
	}
	
	public static String longestSubstring(String str1, String str2) {
		 
		StringBuilder sb = new StringBuilder();
		if (str1 == null || str1.isEmpty() || str2 == null || str2.isEmpty())
		  return "";
		 
		// ignore case
		//str1 = str1.toLowerCase();
		//str2 = str2.toLowerCase();
		 
		// java initializes them already with 0
		int[][] num = new int[str1.length()][str2.length()];
		int maxlen = 0;
		int lastSubsBegin = 0;
		 
		for (int i = 0; i < str1.length(); i++) {
		for (int j = 0; j < str2.length(); j++) {
		  if (str1.charAt(i) == str2.charAt(j)) {
		    if ((i == 0) || (j == 0))
		       num[i][j] = 1;
		    else
		       num[i][j] = 1 + num[i - 1][j - 1];
		 
		    if (num[i][j] > maxlen) {
		      maxlen = num[i][j];
		      // generate substring from str1 => i
		      int thisSubsBegin = i - num[i][j] + 1;
		      if (lastSubsBegin == thisSubsBegin) {
		         //if the current LCS is the same as the last time this block ran
		         sb.append(str1.charAt(i));
		      } else {
		         //this block resets the string builder if a different LCS is found
		         lastSubsBegin = thisSubsBegin;
		         sb = new StringBuilder();
		         sb.append(str1.substring(lastSubsBegin, i + 1));
		      }
		   }
		}
		}}
		 
		return sb.toString();
		}
		
	public static void processWalks(){
		ArrayList<String> walks = new ArrayList<String>(2);
		int numWalk = walkList.size();
		for (int i = 0; i< numWalk ; i++){
			for (int j = i+1 ; j < numWalk ; j++){
				walks = alignWalk(walkList.get(j), walkList.get(i));	
				walkList.set(j, walks.get(0));
				walkList.set(i, walks.get(1));
			}
		}
	}
	
	public static void printWalks() throws FileNotFoundException, UnsupportedEncodingException{
		int numWalk = 0;
		writeOnFile();
		for (String walk : walkList){
			numWalk++;
			if (!(walk == null)){
			if (walk.length() > 0){
				writer.print(">");
				writer.print("Contig Number " + numWalk);
				writer.print("\n");
				writer.print(walk);
				writer.print("\n");
			}
			}
		}
		writer.close();
	}
	public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException{
		
		System.out.println(longestSubstring("ACGTTTCT" , "GTATTTCT"));
		System.out.println(alignWalk("ATTTC" , "GTATTTCT").toString());
		walkList.add("ACGTTTCT");
		walkList.add("GTATTTCT");
		walkList.add("ATTTC");
		processWalks();
		printWalks();
		
	}
}
