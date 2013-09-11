package edu.uci.ics.genomix.pregelix.log;

public class LoggingType {
    
    public static final byte ORIGIN = 0b00;
    public static final byte AFTER_UPDATE = 0b01;
    public static final byte RECEIVE_MSG = 0b10;
    public static final byte SEND_MSG = 0b11;
    
    public static String getContent(byte type){
        switch(type){
            case LoggingType.ORIGIN:
                return "Before any operations:";
            case LoggingType.AFTER_UPDATE:
                return "After update:";
            case LoggingType.RECEIVE_MSG:
                return "Receive message:";
            case LoggingType.SEND_MSG:
                return "Send message: ";
        }
        return null;
        
    }
}
