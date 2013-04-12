package edu.uci.ics.hyracks.algebricks.rewriter.rules;

/*
 * Replace the Hyracks input operator with my own MR input operator
 * First, at the *logical* level looks for a datasourcescan,
 * if found, sets its physical operator to a new physical operator
 * instead of the regular chain: datasource_scan->one2one->empty_tuples_source 
 */

import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.physical.DataSourceScan4MRPOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.physical.Write4MRPOperator;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class ReplaceInputOutputOperator implements IAlgebraicRewriteRule {

	@Override
	public boolean rewritePre(Mutable<ILogicalOperator> opRef,
			IOptimizationContext context) throws AlgebricksException {
		
		AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
		LogicalOperatorTag tag = op.getOperatorTag();

		System.out.println(tag);
		if (tag.equals(LogicalOperatorTag.DATASOURCESCAN)){
			op.setPhysicalOperator(new DataSourceScan4MRPOperator(null));
			List<Mutable<ILogicalOperator>> inputs = op.getInputs();
			inputs.clear();
			System.out.println();
		}

		if (tag.equals(LogicalOperatorTag.WRITE)){ //What is write result??
			op.setPhysicalOperator(new Write4MRPOperator(null));
			System.out.println();
		}
		return false;
	}

	@Override
	public boolean rewritePost(Mutable<ILogicalOperator> opRef,
			IOptimizationContext context) throws AlgebricksException {
		// TODO Auto-generated method stub
		return false;
	}
   
}

