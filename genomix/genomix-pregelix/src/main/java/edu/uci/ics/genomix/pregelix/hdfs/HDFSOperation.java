package edu.uci.ics.genomix.pregelix.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSOperation {

	private static Configuration conf;
	private static FileSystem hdfs;
	private static Path path;
	
	public HDFSOperation() throws IOException{
		conf = new Configuration();
		hdfs = FileSystem.get(conf);
		path = null;
	}
	
	public static boolean insertHDFSFile(String fileName, int length, byte[] buffer) throws IOException{
		path = new Path(fileName);
		if (!hdfs.exists(path))
			createHDFSFile(fileName,length,buffer);
		else
			appendHDFSFile(fileName,length,buffer);
		return true;
	}
	
	public static boolean createHDFSFile(String fileName, int length, byte[] buffer) throws IOException{
		path = new Path(fileName);
		if (hdfs.exists(path)){
			System.out.println("Output already exists");
		    return false;
		}
		/*if (!hdfs.isFile(path)){
			System.out.println("Output should be a file");
		    return false;
		}*/
		FSDataOutputStream outputStream = hdfs.create(path);
		outputStream.writeInt(length);
		outputStream.write(buffer); 
		outputStream.close();
		return true;
	}
	
	public static boolean appendHDFSFile(String fileName, int length, byte[] buffer) throws IOException{
		path = new Path(fileName);
		if (!hdfs.exists(path)){
			System.out.println("Output not found");
		    return false;
		}
		if (!hdfs.isFile(path)){
			System.out.println("Output should be a file");
		    return false;
		}
		FSDataOutputStream outputStream = hdfs.append(path);
		outputStream.writeInt(length);
		outputStream.write(buffer); 
		outputStream.close();
		return true;
	}
	
	public static boolean deleteHDFSFile(String fileName) throws IOException{
		path = new Path(fileName);
		if (!hdfs.exists(path)){
			System.out.println("Input file not found");
			return false;
		}
		if (!hdfs.isFile(path)){
			System.out.println("Input should be a file");
		    return false;
		}
		return hdfs.delete(path,true);
	}
	
	public static boolean copyFromLocalFile(String srcFile, String dstFile) throws IOException{
		Path srcPath = new Path(srcFile);
		path = new Path(dstFile);
		if (!hdfs.exists(path)){
			System.out.println("Input file not found");
			return false;
		}
		if (!hdfs.isFile(path)){
			System.out.println("Input should be a file");
		    return false;
		}
		hdfs.copyFromLocalFile(srcPath, path);
		return true;
	}
	
	public static void testReadAndWriteHDFS() throws Exception{
		String inFileName = "testHDFS/testInput";
		String outFileName = "testHDFS/testOutput";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path inFile = new Path(inFileName);
		Path outFile = new Path(outFileName);
		if (!fs.exists(inFile)){
			System.out.println("Input file not found");
			return;
		}
		if (!fs.isFile(inFile)){
			System.out.println("Input should be a file");
		    return;
		}
		if (fs.exists(outFile)){
			System.out.println("Output already exists");
		    return;
		}
		FSDataInputStream in = fs.open(inFile);
		FSDataOutputStream out = fs.create(outFile);
		byte[] buffer = new byte[1024];
		int bytesRead = 0;
		while ((bytesRead = in.read(buffer)) > 0) {
			out.write(buffer, 0, bytesRead);
		}
		in.close();
		out.close();
	}
}
