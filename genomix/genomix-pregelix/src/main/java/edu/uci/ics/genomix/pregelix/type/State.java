package edu.uci.ics.genomix.pregelix.type;

public class State {
	public static final int NON_VERTEX = 0;
	public static final int START_VERTEX = 1;
	public static final int END_VERTEX = 2;
	public static final int MID_VERTEX = 3;
	public static final int TODELETE = 4;
	public static final int FINAL_VERTEX = 5;
	public static final int FINAL_DELETE = 6;
	public static final int KILL_SELF = 7;
	public static final int NON_EXIST = 8;
	
	public final static class STATE_CONTENT{

		public static String getContentFromCode(int code){
			String r = "";
			switch(code){
			case NON_VERTEX:
				r = "NON_VERTEX";
				break;
			case START_VERTEX:
				r = "START_VERTEX";
				break;
			case END_VERTEX:
				r = "END_VERTEX";
				break;
			case MID_VERTEX:
				r = "MID_VERTEX";
				break;
			case TODELETE:
				r = "TODELETE";
				break;
			case FINAL_VERTEX:
				r = "FINAL_VERTEX";
				break;
			case FINAL_DELETE:
				r = "FINAL_DELETE";
				break;
			case KILL_SELF:
				r = "KILL_SELF";
				break;
			case NON_EXIST:
				r = "NON_EXIST";
				break;
			}
			return r;
		}
	}
}
