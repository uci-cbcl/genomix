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
    public static void printPhysicalOps(ILogicalPlan plan, StringBuilder out, int indent) {
        appendln(out, "digraph G {");
        appendln(out, "     node[style=rounded]");
        
        
        for (Mutable<ILogicalOperator> root : plan.getRoots()) {
            printPhysicalOperator2((AbstractLogicalOperator) root.getValue(), 5, out, counter);
        }
        appendln(out, "\n}");
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

    public static void printPhysicalOperator2(AbstractLogicalOperator op, int indent, StringBuilder out, int counter) {
        
        IPhysicalOperator pOp = op.getPhysicalOperator();
        pad(out, indent);
        
        if (!op.getInputs().isEmpty()){
            
            for (Mutable<ILogicalOperator> i : op.getInputs()) {
                append(out, op.toStringMR()+"_"+counter + " -> ");
                if (op.getInputs().size()==1)
                    counter++;
                AbstractLogicalOperator child = (AbstractLogicalOperator)i.getValue(); 
                appendln(out, child.toStringMR()+"_"+counter);
                printPhysicalOperator2(child, indent, out, counter);
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
