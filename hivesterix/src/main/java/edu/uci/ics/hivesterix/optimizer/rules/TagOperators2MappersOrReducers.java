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
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/*
 * This class prints the Map-Reduce plan with no optimization yet
 * In ConvertAlgrebricks2MapReduce there was a Pair (the counter, the super-node current number)
 * Here a Pair is not necessary as I already see the super-nodes in the annotations
 * I simply keep a counter in TaggingMRCounter to count the number of Map-Reduces in total
 * By the way, this counter should be the same as the number of super-nodes.
 * 
 * */
public class TagOperators2MappersOrReducers implements IAlgebraicRewriteRule {

    private static int counterMR = -1; //counts the number of mappers and reducers overall
    private String tagMapperReducerKey = TagOperators2MappersOrReducers.class.getName()+".LabelProvider";
    private String tagStateAnnotationKey = ConvertAlgebricks2MapReduceRule.class.getName()+".LabelProvider";

    //there's an TaggingMRCounter annotation associated to an operator
    //this class is static as independant of TagOperators2MappersOrReducers
    public static class TaggingMRCounter {
        String label;
        int counter;

        public TaggingMRCounter() {
            label        = "M";
            this.counter =  0;
        }

        public TaggingMRCounter(String label, int counter) {
            this.label   = label;
            this.counter = counter;
        }

        public int getCounter() {
            return counter;
        }

        public String getLabel() {
            return label;
        }

        public void incrementAndSetCounter() {
            this.counter++;
        }
        @Override
        public String toString() {

            return new String(label+"-"+counter);
        }

    }

    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {

        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {

        //Acquire the current node annotations's map
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        Map<String, Object> annotations_current  = op.getAnnotations(); 

        //Checks whether current node has children
        List<Mutable<ILogicalOperator>> children_list= op.getInputs();
        TaggingMRCounter tag_MR_current = null;

        if (children_list.isEmpty()){ 
            tag_MR_current = new TaggingMRCounter("M", ++counterMR);
            annotations_current.put(tagMapperReducerKey, tag_MR_current);

        }
        else{ //i.e. no children

            //Do all children have the same super-node number as current node? 
            //yes: node is tagged like children M(x) or R(x), 
            //no:  node is tagged R(counterMR + 1) 

            Pair tag_super_node_current = null;
            Pair tag_super_node_child   = null;
            Map<String, Object> annotations_child = null;
            boolean inTheSameSuperNode  = true;
            int child_sn_number;

            for(Mutable<ILogicalOperator> child:children_list){ //usually it's one child

                //current's annotations for super-node
                tag_super_node_current = (Pair)annotations_current.get(tagStateAnnotationKey);

                //child's annotations for super-node
                AbstractLogicalOperator child_sp = (AbstractLogicalOperator) child.getValue();
                annotations_child     = child_sp.getAnnotations();
                tag_super_node_child  = (Pair)annotations_child.get(tagStateAnnotationKey);

                //Have we switched super-node number?
                int current_sn_number = Integer.parseInt(tag_super_node_current.second.toString()); //no need to recalculate it every iteration but easier to read
                child_sn_number       = Integer.parseInt(tag_super_node_child.second.toString());

                if (current_sn_number != child_sn_number){
                    inTheSameSuperNode = false;
                }
            }

            if (inTheSameSuperNode){
                tag_MR_current = (TaggingMRCounter) annotations_child.get(tagMapperReducerKey);
            }
            else{
                //consecutive reduces, but should have a different number
                tag_MR_current = new TaggingMRCounter("R", ++counterMR);
            }

            annotations_current.put(tagMapperReducerKey, tag_MR_current);
        }

        //Print the MR state
        System.out.println("node is of type : "+ tag_MR_current +" "+ op);
        return false;
    }
}


