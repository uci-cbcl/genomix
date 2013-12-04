package edu.uci.ics.genomix.pregelix.operator.scaffolding;

import java.util.ArrayList;

import edu.uci.ics.genomix.pregelix.io.vertex.VertexValueWritable;
import edu.uci.ics.genomix.type.VKmer;
import edu.uci.ics.genomix.type.VKmerList;

public class ScaffoldingVertexValueWritable extends VertexValueWritable{
	//Do you really need all of them?
	private static final long serialVersionUID = 1L;
	int rules_a = 0;
	int rules_b = 0;
	int rules_c = 0;
	int walkSize;
	int index;
	VKmerList walk;
	VKmer neighborKmer, lastKmer;
	boolean previsitedFlag;
	boolean doneFlag = false;
	boolean flipFalg =  false;
	boolean startFlag = false;
	public ScaffoldingVertexValueWritable(){
        super();
        walk = new VKmerList();
        lastKmer = new VKmer();
    }
}
