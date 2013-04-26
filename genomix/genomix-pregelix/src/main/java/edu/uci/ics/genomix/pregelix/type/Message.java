package edu.uci.ics.genomix.pregelix.type;

public class Message {
	
	public static final byte NON = 0;
	public static final byte START = 1;
	public static final byte END = 2;
	
	public final static class MESSAGE_CONTENT{
		
		public static String getContentFromCode(byte code){
			String r = "";
			switch(code){
			case NON:
				r = "NON";
				break;
			case START:
				r = "START";
				break;
			case END:
				r = "END";
				break;
			}
			return r;
		}
	}
}
