import java.util.ArrayList;
import java.util.List;

import javax.xml.crypto.dsig.keyinfo.KeyValue;

import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.compiler.api.AbstractCompilerFactoryBuilder;
import edu.uci.ics.hyracks.algebricks.compiler.api.HeuristicCompilerFactoryBuilder;
import edu.uci.ics.hyracks.algebricks.compiler.api.ICompiler;
import edu.uci.ics.hyracks.algebricks.compiler.api.ICompilerFactory;
import edu.uci.ics.hyracks.algebricks.compiler.rewriter.rulecontrollers.SequentialFixpointRuleController;
import edu.uci.ics.hyracks.algebricks.compiler.rewriter.rulecontrollers.SequentialOnceRuleController;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IExpressionEvalSizeComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IExpressionRuntimeProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IExpressionTypeComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.INullableTypeComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IPartialAggregationTypeComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSink;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSource;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSourceIndex;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.WriteOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.plan.ALogicalPlanImpl;
import edu.uci.ics.hyracks.algebricks.core.algebra.typing.ITypingContext;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobBuilder;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.PlanCompiler;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.AbstractRuleController;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import edu.uci.ics.hyracks.algebricks.data.IBinaryBooleanInspectorFactory;
import edu.uci.ics.hyracks.algebricks.data.IBinaryComparatorFactoryProvider;
import edu.uci.ics.hyracks.algebricks.data.IBinaryHashFunctionFactoryProvider;
import edu.uci.ics.hyracks.algebricks.data.IBinaryHashFunctionFamilyProvider;
import edu.uci.ics.hyracks.algebricks.data.IBinaryIntegerInspectorFactory;
import edu.uci.ics.hyracks.algebricks.data.INormalizedKeyComputerFactoryProvider;
import edu.uci.ics.hyracks.algebricks.data.IPrinterFactory;
import edu.uci.ics.hyracks.algebricks.data.IPrinterFactoryProvider;
import edu.uci.ics.hyracks.algebricks.data.ISerializerDeserializerProvider;
import edu.uci.ics.hyracks.algebricks.data.ITypeTraitProvider;
import edu.uci.ics.hyracks.algebricks.runtime.operators.std.StreamSelectRuntimeFactory;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriterFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hadoop.io.Writable;

public class TestHyracksSelect {
	private static List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> buildDefaultLogicalRewrites() {
		List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> defaultLogicalRewrites = new ArrayList<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>>();
		SequentialFixpointRuleController seqCtrlNoDfs = new SequentialFixpointRuleController(
				false);
		SequentialFixpointRuleController seqCtrlFullDfs = new SequentialFixpointRuleController(
				true);
		SequentialOnceRuleController seqOnceCtrl = new SequentialOnceRuleController(
				true);
		return defaultLogicalRewrites;
	}

	private static List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> buildDefaultPhysicalRewrites() {
		List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> defaultPhysicalRewrites = new ArrayList<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>>();
		SequentialOnceRuleController seqOnceCtrlAllLevels = new SequentialOnceRuleController(
				true);
		SequentialOnceRuleController seqOnceCtrlTopLevel = new SequentialOnceRuleController(
				false);
		return defaultPhysicalRewrites;
	}

	public static void main(String[] args) {
		List<String> variables = new ArrayList();
		variables.add(new String("int"));
		
		
		Schema schema = new Schema(variables, );
		IDataSink dataSink = new HiveDataSink(new FileSinkOperator(), schema ); //Schema provided by Hive or Piglet
		List<Mutable<ILogicalExpression>> expressions = null;
		Mutable<ILogicalExpression> condition = null;
		List<LogicalVariable> variables = null;
		IDataSource<?> dataSource = null;

		// TODO: Fill these ones:
		AbstractCompilerFactoryBuilder context = null;// ???
		IOperatorSchema outerPlanSchema = null;

		// roots contain a write->select->data, creating these operators below.
		// [A query can have multiple roots - not the case here. I still kept a
		// list structure]
		List<Mutable<ILogicalOperator>> roots = new ArrayList<Mutable<ILogicalOperator>>();
		WriteOperator write = new WriteOperator(expressions, dataSink);
		DataSourceScanOperator dataScan = new DataSourceScanOperator(variables,
				dataSource);
		SelectOperator select = new SelectOperator(condition);

		// chaining them: scan to select and select to write
		select.getInputs().add(new MutableObject<ILogicalOperator>(dataScan));
		write.getInputs().add(new MutableObject<ILogicalOperator>(select));
		roots.add(new MutableObject<ILogicalOperator>(write));

		// Two steps:
		// 1. create a compiler object which performs the optimization on the
		// plan

		HeuristicCompilerFactoryBuilder builder = new HeuristicCompilerFactoryBuilder();
		builder.setLogicalRewrites(buildDefaultLogicalRewrites());
		builder.setPhysicalRewrites(buildDefaultPhysicalRewrites());
		final ICompilerFactory cFactory = builder.create();

		// 2. create the logican plan based on the roots above, to pass to
		// createCompiler
		ALogicalPlanImpl plan = new ALogicalPlanImpl(roots);

		IMetadataProvider metaData = new IMetadataProvider() {

			@Override
			public IDataSource findDataSource(Object id)
					throws AlgebricksException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public Pair getScannerRuntime(IDataSource dataSource,
					List scanVariables, List projectVariables,
					boolean projectPushed, IOperatorSchema opSchema,
					IVariableTypeEnvironment typeEnv, JobGenContext context,
					JobSpecification jobSpec) throws AlgebricksException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public boolean scannerOperatorIsLeaf(IDataSource dataSource) {
				// TODO Auto-generated method stub
				return false;
			}

			@Override
			public Pair getWriteFileRuntime(IDataSink sink, int[] printColumns,
					IPrinterFactory[] printerFactories,
					RecordDescriptor inputDesc) throws AlgebricksException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public Pair getWriteResultRuntime(IDataSource dataSource,
					IOperatorSchema propagatedSchema, List keys,
					LogicalVariable payLoadVar, JobGenContext context,
					JobSpecification jobSpec) throws AlgebricksException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public Pair getInsertRuntime(IDataSource dataSource,
					IOperatorSchema propagatedSchema, List keys,
					LogicalVariable payLoadVar, RecordDescriptor recordDesc,
					JobGenContext context, JobSpecification jobSpec)
					throws AlgebricksException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public Pair getDeleteRuntime(IDataSource dataSource,
					IOperatorSchema propagatedSchema, List keys,
					LogicalVariable payLoadVar, RecordDescriptor recordDesc,
					JobGenContext context, JobSpecification jobSpec)
					throws AlgebricksException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public Pair getIndexInsertRuntime(IDataSourceIndex dataSource,
					IOperatorSchema propagatedSchema,
					IOperatorSchema[] inputSchemas,
					IVariableTypeEnvironment typeEnv, List primaryKeys,
					List secondaryKeys, ILogicalExpression filterExpr,
					RecordDescriptor recordDesc, JobGenContext context,
					JobSpecification spec) throws AlgebricksException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public Pair getIndexDeleteRuntime(IDataSourceIndex dataSource,
					IOperatorSchema propagatedSchema,
					IOperatorSchema[] inputSchemas,
					IVariableTypeEnvironment typeEnv, List primaryKeys,
					List secondaryKeys, ILogicalExpression filterExpr,
					RecordDescriptor recordDesc, JobGenContext context,
					JobSpecification spec) throws AlgebricksException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public IDataSourceIndex findDataSourceIndex(Object indexId,
					Object dataSourceId) throws AlgebricksException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public IFunctionInfo lookupFunction(FunctionIdentifier fid) {
				// TODO Auto-generated method stub
				return null;
			}

		};

		ICompiler compiler = cFactory.createCompiler(plan, metaData, 1); // KIS
		try {
			compiler.optimize();
		} catch (AlgebricksException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} // this calls does the rewrites of the rules on the plan, which rules
			// you want to execute here??

		// up to now we built a physical optimized plan, now to the runtime part
		// of it.

		IExpressionRuntimeProvider expressionRuntimeProvider = null;
		ISerializerDeserializerProvider serializerDeserializerProvider = null;
		IBinaryHashFunctionFamilyProvider hashFunctionFamilyProvider = null;
		IExpressionTypeComputer expressionTypeComputer = null;
		IOperatorSchema outerFlowSchema = null;
		IPartialAggregationTypeComputer partialAggregationTypeComputer = null;
		IBinaryComparatorFactoryProvider comparatorFactoryProvider = null;
		INormalizedKeyComputerFactoryProvider normalizedKeyComputerFactoryProvider = null;
		IBinaryHashFunctionFactoryProvider hashFunctionFactoryProvider = null;
		Object appContext = null;
		IPrinterFactoryProvider printerFactoryProvider = null;
		INullWriterFactory nullWriterFactory = null;
		IBinaryIntegerInspectorFactory integerInspectorFactory = null;
		ITypeTraitProvider typeTraitProvider = null;
		int frameSize = 0;
		AlgebricksPartitionConstraint clusterLocations = null;
		INullableTypeComputer nullableTypeComputer = null;
		IExpressionEvalSizeComputer expressionEvalSizeComputer = null;
		IBinaryBooleanInspectorFactory booleanInspectorFactory = null;
		ITypingContext typingContext = null;
		// Here will actually start the modification to build the MR plan
		// umbrella for all the runtime properties are stored in jobGenContext
		// later passed to PlanCompiler
		JobGenContext jobGenContext = new JobGenContext(outerFlowSchema,
				metaData, appContext, serializerDeserializerProvider,
				hashFunctionFactoryProvider, hashFunctionFamilyProvider,
				comparatorFactoryProvider, typeTraitProvider,
				booleanInspectorFactory, integerInspectorFactory,
				printerFactoryProvider, nullWriterFactory,
				normalizedKeyComputerFactoryProvider,
				expressionRuntimeProvider, expressionTypeComputer,
				nullableTypeComputer, typingContext,
				expressionEvalSizeComputer, partialAggregationTypeComputer,
				frameSize, clusterLocations);
		PlanCompiler pc = new PlanCompiler(jobGenContext);

		// Wrap the three operators above into an ILogicalPlan - done (plan)
		JobSpecification spec = null;
		try {
			spec = pc.compilePlan(plan, outerPlanSchema);
		} catch (AlgebricksException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// Simulate Hyracks planning to create IoperatorNodePushable, once I
		// have these
/*
		IHyracksJobBuilder jobBuilder = new JobBuilder(spec,
				context.getClusterLocations());
		for (Mutable<ILogicalOperator> opRef : plan.getRoots()) {
			try {
				pc.compileOpRef(opRef, spec, jobBuilder, outerPlanSchema);
			} catch (AlgebricksException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			roots.add((Mutable<ILogicalOperator>) opRef.getValue()); // why is a
																		// cast
																		// mandatory
																		// here??
		//}*/

	}
}
