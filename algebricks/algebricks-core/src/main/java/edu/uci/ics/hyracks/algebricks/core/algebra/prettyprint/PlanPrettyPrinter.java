/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.algebricks.core.algebra.prettyprint;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IPhysicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;

public class PlanPrettyPrinter {
    
    public static void printPlan(ILogicalPlan plan, StringBuilder out, LogicalOperatorPrettyPrintVisitor pvisitor,
            int indent) throws AlgebricksException {
        for (Mutable<ILogicalOperator> root : plan.getRoots()) {
            printOperator((AbstractLogicalOperator) root.getValue(), out, pvisitor, indent);
        }
    }
    static int counter = 0;
    static int counterMI = 0;
    static Random randomGenerator = new Random();
    public static void printPhysicalOps(ILogicalPlan plan, StringBuilder out, int indent) {
        appendln(out, "digraph G {");

        int randomInt = 10000 + randomGenerator.nextInt(100);
               
        for (Mutable<ILogicalOperator> root : plan.getRoots()) {
        	pad(out, indent);
        	printVisualizationGraph((AbstractLogicalOperator) root.getValue(), 5, out, counter, "", randomInt);
        }
        appendln(out, "\n}\n}");
        File file = new File("/home/kereno/dot.txt");
        try {
            FileUtils.writeStringToFile(file, out.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void printOperator(AbstractLogicalOperator op, StringBuilder out,
            LogicalOperatorPrettyPrintVisitor pvisitor, int indent) throws AlgebricksException {
        out.append(op.accept(pvisitor, indent));
        IPhysicalOperator pOp = op.getPhysicalOperator();

        if (pOp != null) {
            out.append("\n");
            pad(out, indent);
            appendln(out, "-- " + pOp.toString() + "  |" + op.getExecutionMode() + "|");
        } else {
            appendln(out, " -- |" + op.getExecutionMode() + "|");
        }

        for (Mutable<ILogicalOperator> i : op.getInputs()) {
            printOperator((AbstractLogicalOperator) i.getValue(), out, pvisitor, indent + 2);
        }

    }

    public static void printPhysicalOperator(AbstractLogicalOperator op, int indent, StringBuilder out) {
        IPhysicalOperator pOp = op.getPhysicalOperator();
        pad(out, indent);
        appendln(out, "-- " + pOp.toString() + "  |" + op.getExecutionMode() + "|");
        if (op.hasNestedPlans()) {
            AbstractOperatorWithNestedPlans opNest = (AbstractOperatorWithNestedPlans) op;
            for (ILogicalPlan p : opNest.getNestedPlans()) {
                pad(out, indent + 8);
                appendln(out, "{");
                printPhysicalOps(p, out, indent + 10);
                pad(out, indent + 8);
                appendln(out, "}");
            }
        }

        for (Mutable<ILogicalOperator> i : op.getInputs()) {
            printPhysicalOperator((AbstractLogicalOperator) i.getValue(), indent + 2, out);
        }

    }

    /*
     * DFS traversal function. Calls iteratively all sons, and for each son calls itself recursively 
     * Includes slim-maps only (no gathering of mappers to one)
     * */
    public static void printVisualizationGraph(AbstractLogicalOperator op, int indent, StringBuilder out, int counter, String current_supernode_name, int randomInt) {
    
    	
    	if (!op.getInputs().isEmpty()){
        	String stringToVisualize = op.toStringForVisualizationGraph(); 
        	int firstOccurenceOf_ = stringToVisualize.indexOf("_");
        	String supernode_current = op.toStringForVisualizationGraph().substring(firstOccurenceOf_+1, stringToVisualize.length());
        	if (current_supernode_name.isEmpty()){
        		current_supernode_name = supernode_current;
        		appendln(out, new String("subgraph cluster_"+supernode_current+" {"));
        		pad(out, indent);
        		appendln(out, new String("node [style=filled, color = pink];"));
        		pad(out, indent);
        		appendln(out, new String("color=blue;"));
        		pad(out, indent);
        		appendln(out, new String("label = \"" + supernode_current+"\";"));
        		pad(out, indent);
        	}
        	boolean eraseExtraVertex = false;
        	for (Mutable<ILogicalOperator> i : op.getInputs()) {
        		String logOpStr = ((AbstractLogicalOperator)i.getValue()).toStringForVisualizationGraph();
        		firstOccurenceOf_ = logOpStr.indexOf("_");
        		String supernode_child = logOpStr.substring(firstOccurenceOf_+1, logOpStr.length());
        		String missingVertex="";
            	if (!supernode_current.equals(supernode_child)){
            		String currentNodeReducerOrMapper = supernode_current.substring(0,1);
            		String childNodeReducerOrMapper = supernode_child.substring(0,1);
            		boolean slim_map = currentNodeReducerOrMapper.equals(childNodeReducerOrMapper) && childNodeReducerOrMapper.equals("R"); 
            		if (slim_map){
            			
            			appendln(out, new String("}"));
                		pad(out, indent);
                		appendln(out, new String("subgraph cluster_slim_map_"+randomInt+" {"));
                		pad(out, indent);
                		appendln(out, new String("node [style=filled, color = pink];"));
                		pad(out, indent);
                		appendln(out, new String("color=blue"));
                		pad(out, indent);
                		appendln(out, new String("label = \"SLIM-MAP"+"\";"));
                		pad(out, indent);
                		appendln(out, new String(stringToVisualize+"_"+counter+"->"+"slim_map_"+counter));
                		
                	}
            		if(slim_map){
            			pad(out, indent);
                	}
            		
            		appendln(out, new String("}"));
            		pad(out, indent);
            		appendln(out, new String("subgraph cluster_"+supernode_child+" {"));
            		pad(out, indent);
            		appendln(out, new String("node [style=filled, color = pink];"));
            		pad(out, indent);
            		appendln(out, new String("color=blue"));
            		pad(out, indent);
            		appendln(out, new String("label = \"" + supernode_child+"\";"));
            		pad(out, indent);
            	
            		
            		if(childNodeReducerOrMapper.equals("R")){
	            		appendln(out, new String("slim_map_"+counter+" -> "+logOpStr+"_"+counter));
	            		pad(out, indent);
	            		eraseExtraVertex = true;
	            	}
            		else if (childNodeReducerOrMapper.equals("M")){
            		    if (op.getInputs().size()>1){
	            	    	List<Mutable<ILogicalOperator>> list= op.getInputs();
	            	    	String left = ((AbstractLogicalOperator)(list.get(0).getValue())).toStringForVisualizationGraph();
	            	    	String left_category = left.substring(left.indexOf("_")+1,left.length());
	            	    	String right = ((AbstractLogicalOperator)(list.get(1).getValue())).toStringForVisualizationGraph();
	            	    	String right_category = right.substring(left.indexOf("_")+1,right.length());
	            	    	
	            	    	if (right_category.startsWith("M") && left_category.startsWith("R")){
	            	    		missingVertex = op.toStringForVisualizationGraph()+"_"+counter + " -> " + right+"_"+counter;
	            	    		
	            	    	}	    		            
	            	    }	
            		}
            	}
            	
            	appendln(out, op.toStringForVisualizationGraph()+"_"+counter + "[style = filled]");
            	AbstractLogicalOperator child = (AbstractLogicalOperator)i.getValue(); 
	            
            	if (!eraseExtraVertex){
	            	pad(out, indent);
	            	append(out, op.toStringForVisualizationGraph()+"_"+counter + " -> ");
	                if (op.getInputs().size()==1){
	                    counter++;
	                }
	                appendln(out, child.toStringForVisualizationGraph()+"_"+counter);
	                
            	}
	            
            	if (!missingVertex.isEmpty()){
                	appendln(out, missingVertex);
                }
            	
            	printVisualizationGraph(child, indent, out, counter, supernode_current, (randomGenerator.nextInt(100)+10000));
            
        	 }
        }
    }

    private static void appendln(StringBuilder buf, String s) {
        buf.append(s);
        buf.append("\n");
        
    }

    private static void append(StringBuilder buf, String s) {
        buf.append(s);
    }

    private static void pad(StringBuilder buf, int indent) {
        for (int i = 0; i < indent; ++i) {
            buf.append(' ');
        }
    }
}
