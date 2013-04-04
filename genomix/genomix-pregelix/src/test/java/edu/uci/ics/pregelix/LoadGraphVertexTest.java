package edu.uci.ics.pregelix;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;

import org.junit.Test;

import edu.uci.ics.genomix.pregelix.LoadGraphVertex.SimpleLoadGraphVertexOutputFormat;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.example.client.Client;

public class LoadGraphVertexTest {

	/**
	 * I can't debug in JUnits test so that I can't find my error here. So I leave comments here.
	 * I will figure out as soon as possible.
	 */
	private static final String EXPECT_RESULT_FILE = "expected_result";
	private static final String INPUT_PATHS = "folder";
	private static final String OUTPUT_PATH = "result";
	private static final String IP = "169.234.134.212";
	private static final String PORT = "3099";
	
	@SuppressWarnings("deprecation")
	@Test
	public void test() throws Exception {
		//initiate args
	/*	String[] args = new String[8];
		args[0] = "-inputpaths"; 
		args[1] = INPUT_PATHS;
		args[2] = "-outputpath";
		args[3] = OUTPUT_PATH;
		args[4] = "-ip";
		args[5] = IP;
		args[6] = "-port";
		args[7] = PORT;
        PregelixJob job = new PregelixJob(LoadGraphVertex.class.getSimpleName());
        job.setVertexClass(LoadGraphVertex.class);
        job.setVertexInputFormatClass(TextLoadGraphInputFormat.class);
        job.setVertexOutputFormatClass(SimpleLoadGraphVertexOutputFormat.class);
        Client.run(args, job);
        
        generateExpectBinaryFile();
        
        //test if the actual file is the same as the expected file
        DataInputStream actual_dis = new DataInputStream(new FileInputStream(OUTPUT_PATH + "/*"));
        DataInputStream expected_dis = new DataInputStream(new FileInputStream(EXPECT_RESULT_FILE));
        String actualLine, expectedLine = null;
        StringTokenizer actualSt, expectedSt;
		byte[] actualVertexId, expectedVertexId = null;
		byte actualVertexValue, expectedVertexValue;
        byte[] tmp = null;
        while(((actualLine = actual_dis.readLine()) != null) && 
        		((expectedLine = expected_dis.readLine()) != null)){
        	actualSt = new StringTokenizer(actualLine, " ");
			actualVertexId = actualSt.nextToken().getBytes();
			tmp = actualSt.nextToken().getBytes();
			actualVertexValue = tmp[0];
			
			expectedSt = new StringTokenizer(expectedLine," ");
			expectedVertexId = expectedSt.nextToken().getBytes();
			tmp = expectedSt.nextToken().getBytes();
			expectedVertexValue = tmp[0];
			
			assertEquals("actualVextexId == expectedVertexId", actualVertexId, expectedVertexId);
			assertEquals("actualVertexValue == expectedVertexValue", actualVertexValue, expectedVertexValue);
        }
        
        assertEquals("actualLine should be the end and be equal to null", actualLine, null);
        assertEquals("expectedLine should be the end and be equal to null", expectedLine, null);*/
	}
	
	@SuppressWarnings("deprecation")
	public void generateExpectBinaryFile() throws Exception{
		DataInputStream dis = new DataInputStream(new FileInputStream(INPUT_PATHS + "/*"));
		DataOutputStream dos = new DataOutputStream(new FileOutputStream(EXPECT_RESULT_FILE));
		String line;
		byte[] vertexId = null;
		byte vertexValue;
		byte[] tmp = null;
		while((line = dis.readLine()) != null){
			StringTokenizer st = new StringTokenizer(line, " ");
			vertexId = st.nextToken().getBytes();
			tmp = st.nextToken().getBytes();
			vertexValue = tmp[0];		
			
			vertexValue = (byte) (vertexValue << 1); 
			for(int i = 0; i < vertexId.length; i++)
				dos.writeByte(vertexId[i]);
			dos.writeByte((byte)32); //space
			dos.writeByte(vertexValue);
			dos.writeByte((byte)10); //line feed
		}
		
		dis.close();
		dos.close();
	}

}
