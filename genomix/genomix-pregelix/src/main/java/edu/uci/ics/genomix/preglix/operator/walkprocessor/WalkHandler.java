package edu.uci.ics.genomix.preglix.operator.walkprocessor;

import java.util.ArrayList;

import edu.uci.ics.genomix.data.types.VKmerList;

public class WalkHandler {
	private static final int MIN_OVERLAPSIZE = 4;

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
}
