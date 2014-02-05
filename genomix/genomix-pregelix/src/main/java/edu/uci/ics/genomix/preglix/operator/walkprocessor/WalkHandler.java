package edu.uci.ics.genomix.preglix.operator.walkprocessor;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.io.FileUtils;

import edu.uci.ics.genomix.data.types.EDGETYPE;
import edu.uci.ics.genomix.data.types.VKmer;
import edu.uci.ics.genomix.data.types.VKmerList;

public class WalkHandler {
	private static final int MIN_OVERLAPSIZE = 4;
	private static final int kmerSize = 21;
	private static HashMap <VKmer, VKmerList> walkMap = new HashMap<VKmer, VKmerList>();
	
	/**
	 * Reads all .txt file from a directory and build a hashmap for walks.
	 * @param directory
	 * @return
	 * @throws IOException
	 */
	public HashMap <VKmer, VKmerList> loadWalkMap(final File directory) throws IOException{
		VKmerList walk = new VKmerList();
		VKmer node = new VKmer();
		for (final File file : directory.listFiles()) {
			walk.clear();
	        if (!file.isDirectory()) {
	        	String content = FileUtils.readFileToString(file);
	        	String [] words = content.split("[\\W]");
	        	for (String word : words){
	        		node.setAsCopy(word);
	        		walk.append(node);;
	        	}
	        	node.setAsCopy(file.getName());
	        	walkMap.put(node, walk);
	        }
	    }
		
		return walkMap;
		
	}
	/**
	 * This method gives back the walk for each seed.
	 * However for now, there is no good implementation to store the walks.
	 * @param id
	 * @return
	 */
	public VKmerList loadWalk(VKmer id){
		return walkMap.get(id);
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
	
	public VKmer buildAWalk(VKmerList walk){
		boolean[] walkDir = new boolean[walk.size()]; 
		EDGETYPE edge;
		EDGETYPE tmp = findEdge(walk.getPosition(0), walk.getPosition(1));
		boolean checkEdge;
		for (int i = 0; i < walk.size()-2; i++){
			edge = findEdge(walk.getPosition(i), walk.getPosition(i+1));
			checkEdge  = checkFoundEdge(edge, tmp);
			tmp = checkEdge ? edge : edge.mirror();
			
		}
		return null;
		
	}
	
	//NotWorking
	//Need to do it another way
	
	public EDGETYPE findEdge(VKmer vkmer1, VKmer vkmer2) {
		
		if(vkmer1.toString().regionMatches(1, vkmer2.toString(), 0, kmerSize - 1)){
			return EDGETYPE.FF;
		}else if (vkmer1.toString().regionMatches(1, vkmer2.reverse().toString(), 0, kmerSize - 1)){
			return EDGETYPE.FR;
		}else if (vkmer1.reverse().toString().regionMatches(1, vkmer2.toString(), 0, kmerSize - 1 )){
			return EDGETYPE.RF;
		}else if (vkmer1.reverse().toString().regionMatches(1, vkmer2.reverse().toString(), 0, kmerSize -1)){
			return EDGETYPE.RR;
		}
		
			return null;
	}
	
	public boolean checkFoundEdge(EDGETYPE edge1, EDGETYPE edge2){
		if (edge1 == EDGETYPE.FF || edge1 == EDGETYPE.RF){
			if (edge2 == EDGETYPE.FF || edge2 == EDGETYPE.FR){
				return true;
			}else {
				return false;
			}
		} 
		if (edge1 == EDGETYPE.FR || edge1 == EDGETYPE.RR){
			if (edge2 == EDGETYPE.RF || edge2 == EDGETYPE.RR){
				return true;
			}else {
				return false;
			}
		}		
		
		return false;
		
	}
}
