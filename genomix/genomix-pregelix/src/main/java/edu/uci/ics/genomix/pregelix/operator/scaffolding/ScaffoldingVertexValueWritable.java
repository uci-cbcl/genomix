package edu.uci.ics.genomix.pregelix.operator.scaffolding;

import java.util.ArrayList;

import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.type.VKmer;

public class ScaffoldingVertexValueWritable extends VertexValueWritable{
<<<<<<< HEAD
	private static final long serialVersionUID = 1L;
=======
	
>>>>>>> 9e811bd7dd411531254df89d8ef755146993e28b
	int rules_a, rulep_a;
	int rules_b, rulep_b;
	int rules_c, rulep_c;
	int walkSize;
	int index;
	VKmer neighborKmer, lastKmer;
	ArrayList<VKmer> walk;
	boolean previsitedFlag;
	boolean doneFlag;
<<<<<<< HEAD
	boolean flipFalg;
	boolean startFlag;
=======

>>>>>>> 9e811bd7dd411531254df89d8ef755146993e28b
	public ScaffoldingVertexValueWritable(){
        super();
    }

	
}
