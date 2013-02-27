package edu.uci.ics.genomix.dataflow.util;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.SequenceFile.ValueBytes;

public class NonSyncWriter {
	private FSDataOutputStream out;

	public NonSyncWriter(FSDataOutputStream output) {
		out = output;
	}

	public void appendRaw(byte[] keyData, int keyOffset, int keyLength,
			ValueBytes val) throws IOException {
		out.writeInt(keyLength + val.getSize()); // total record length

		out.writeInt(keyLength); // key portion length

		out.write(keyData, keyOffset, keyLength); // key

		val.writeUncompressedBytes(out); // value
	}

}
