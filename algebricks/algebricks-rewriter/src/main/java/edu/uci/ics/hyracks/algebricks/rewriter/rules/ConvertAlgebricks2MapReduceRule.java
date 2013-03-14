package edu.uci.ics.hyracks.algebricks.rewriter.rules;

/*
 * Very similar to ConvertAlgebricks2MapReduceRule of Pigsterix (in pigsterix.googlecode.com) aside of:
 * - deletion of rewritePost for assigns (only relevant to Pig)
 * - utility classes are inner classes this time
 */

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IPhysicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.TaggingMRCounter;

public class ConvertAlgebricks2MapReduceRule implements IAlgebraicRewriteRule {
        
    //there's an TaggingSNCounter annotation associated to an operator
    //this class is static as independant of ConvertAlgebricks2MapReduceRule
    public static class TaggingSNState {

        int label;
        
        public TaggingSNState() {
            this.label = 0;
        }

        public TaggingSNState(int label) {
            this.label = label;
        }
        
        public int getLabel() {
            return label;
        }
        
        public void incrementAndSetLabel() {
            this.label++;
        }
    }
    
	/*
     * This function traverses the tree of logical operator and annotates the operators in group of super-nodes.
     * For each operator it sets in its annotations (map structures) a new K,V respectively tagStateAnnotationKey, Pair
     * The pair contains two components first and second:
     * first -  a counter of how many super-nodes were encountered so far
     * second - the super-node number for this operator
     * NB: In this function, I only set the pair for the current node and its immediate children. 
     * rewritePre handles the traversal for the entire tree.
     * */
	@Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {

		AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
		IPhysicalOperator opPhy = op.getPhysicalOperator();
		PhysicalOperatorTag tag = opPhy.getOperatorTag();

		Map<String, Object> annotations = op.getAnnotations();
		Pair tag_parent = (Pair)annotations.get(TaggingMRCounter.tagSuperNodeKey);
		
		//at the root level, if the annotation doesn't exist then creates it
		if (tag_parent == null) {
			tag_parent = new Pair (new TaggingSNState(), 0);
			annotations.put(TaggingMRCounter.tagSuperNodeKey, tag_parent);
		} 
		System.out.println("super-node: #"+ tag_parent.second +", counter: #" +((TaggingSNState)tag_parent.first).getLabel() +" "+ op);
		
		//iterates on the children and point to the parent's annotation
		List<Mutable<ILogicalOperator>> children_list= op.getInputs();
				
		for(Mutable<ILogicalOperator> child:children_list){
			AbstractLogicalOperator child_sp = (AbstractLogicalOperator) child.getValue();
			Map<String, Object> annotations_child  = child_sp.getAnnotations();
			
			IPhysicalOperator opPhy1 = child_sp.getPhysicalOperator();
			PhysicalOperatorTag tag1 = opPhy1.getOperatorTag();
			
			boolean exchange = ((tag1 == PhysicalOperatorTag.HASH_PARTITION_EXCHANGE) || 
			   (tag1 == PhysicalOperatorTag.BROADCAST_EXCHANGE) ||
			   (tag1 == PhysicalOperatorTag.HASH_PARTITION_MERGE_EXCHANGE) ||
			   (tag1 == PhysicalOperatorTag.RANGE_PARTITION_EXCHANGE) ||
			   (tag1 == PhysicalOperatorTag.RANDOM_MERGE_EXCHANGE) ||
			   (tag1 == PhysicalOperatorTag.SORT_MERGE_EXCHANGE));
			
			if (exchange){
				((TaggingSNState)(tag_parent.first)).incrementAndSetLabel();
				Pair p = new Pair(tag_parent.first, ((TaggingSNState)tag_parent.first).getLabel());
				annotations_child.put(TaggingMRCounter.tagSuperNodeKey, p);
				//System.out.println("super-node: #" + p.second +", counter: #"+((TaggingSNState)p.first).getLabel()+" exchange operator");
			}
	
			else{
				annotations_child.put(TaggingMRCounter.tagSuperNodeKey, tag_parent);
			}
		}
  	
    	return false;
    }
	
    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        
        return false;
    }
}

