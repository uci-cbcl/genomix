package edu.uci.ics.genomix.pregelix.operator.scaffolding;

import java.util.ArrayList;

import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.type.VKmer;

public class ScaffoldingVertexValueWritable extends VertexValueWritable{
	
	int rules_a, rulep_a;
	int rules_b, rulep_b;
	int rules_c, rulep_c;
	int walkSize;
	int index;
	VKmer neighborKmer, lastKmer;
	ArrayList<VKmer> walk;
	
	boolean previsitedFlag;

	public ScaffoldingVertexValueWritable(){
        super();
    }
	
	

}
