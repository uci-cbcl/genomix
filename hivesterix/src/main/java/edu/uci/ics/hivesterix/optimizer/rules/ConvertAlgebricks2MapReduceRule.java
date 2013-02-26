package edu.uci.ics.hivesterix.optimizer.rules;

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
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class ConvertAlgebricks2MapReduceRule implements IAlgebraicRewriteRule {
    public static class PigConstantValue implements IAlgebricksConstantValue{

        private Object value;
        private Object type;
        
        public PigConstantValue(Object value, Object type){
            this.value = value;
            this.type = type;
        }
        
        @Override
        public boolean isNull() {
            // TODO Auto-generated method stub
            return false;
        }

        @Override
        public boolean isTrue() {
            // TODO Auto-generated method stub
            return false;
        }

        @Override
        public boolean isFalse() {
            return false;
        }
        
        public Object getValue() {
            return value;
        }

        public void setValue(Object value) {
            this.value = value;
        }

        public Object getType() {
            return type;
        }

        public void setType(Object type) {
            this.type = type;
        }


    }
    public static class TaggingState {

        int label;
        
        public TaggingState() {
            this.label = 0;
        }
        
        public TaggingState(int label) {
            this.label = label;
        }


        public int getLabel() {
            return label;
        }

        public void incrementAndSetLabel() {
            this.label++;
        }
        
        
    }
	private String tagStateAnnotationKey = ConvertAlgebricks2MapReduceRule.class.getName()+".LabelProvider";
    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {

		AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
		IPhysicalOperator opPhy = op.getPhysicalOperator();
		PhysicalOperatorTag tag = opPhy.getOperatorTag();

		Map<String, Object> annotations = op.getAnnotations();
		Pair tag_parent = (Pair)annotations.get(tagStateAnnotationKey);
		//at the root level, if the annotation doesnt exist - create it
		if (tag_parent == null) {
			tag_parent = new Pair (new TaggingState(), 0);
			annotations.put(tagStateAnnotationKey, tag_parent);
		} 
		System.out.println("super-node: #"+ tag_parent.second +", counter: #" +((TaggingState)tag_parent.first).getLabel() +" "+ op);
		//iterate on the children and point to the parent annotation
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
				((TaggingState)(tag_parent.first)).incrementAndSetLabel();
				Pair p = new Pair(tag_parent.first, ((TaggingState)tag_parent.first).getLabel());
				annotations_child.put(tagStateAnnotationKey, p);
				System.out.println("super-node: #" + p.second +", counter: #"+((TaggingState)p.first).getLabel()+" exchange operator");
			
			}
			else{
				annotations_child.put(tagStateAnnotationKey, tag_parent);
			}
			
		}
  	
    	return false;
    }
    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        // TODO Auto-generated method stub
        return false;
    }
}

