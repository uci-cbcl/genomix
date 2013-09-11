package edu.uci.ics.genomix.pregelix.log;

public class LoggingType {
    
    public static final byte ORIGIN = 0b00 << 0;
    
    public static String getContent(byte type){
        switch(type){
            case LoggingType.ORIGIN:
                return "Before any operations";
        }
        return null;
        
    }
}
