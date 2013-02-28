package edu.uci.ics.pregelix.bitwise;

public class BitwiseOperation {
	
	public static byte[] shiftBitsLeft(byte[] bytes, final int leftShifts) {
	   assert leftShifts >= 1 && leftShifts <= 7;

	   byte[] resultBytes = new byte[bytes.length];
	   for(int i = 0; i < bytes.length; i++)
		   resultBytes[i] = bytes[i];
	   final int rightShifts = 8 - leftShifts;

	   byte previousByte = resultBytes[bytes.length - 1]; // keep the byte before modification
	   resultBytes[resultBytes.length - 1] = (byte) (((resultBytes[resultBytes.length - 1] & 0xff) << leftShifts));
	   for (int i = resultBytes.length - 2; i >= 0; i--) {
	      byte tmp = resultBytes[i];
	      resultBytes[i] = (byte) (((resultBytes[i] & 0xff) << leftShifts) | ((previousByte & 0xff) >> rightShifts));
	      previousByte = tmp;
	   }
	   return resultBytes;
	}
	
	public static byte[] shiftBitsRight(byte[] bytes, final int rightShifts) {
	   assert rightShifts >= 1 && rightShifts <= 7;

	   byte[] resultBytes = new byte[bytes.length];
	   for(int i = 0; i < bytes.length; i++)
		   resultBytes[i] = bytes[i];
	   final int leftShifts = 8 - rightShifts;

	   byte previousByte = resultBytes[0]; // keep the byte before modification
	   resultBytes[0] = (byte) (((resultBytes[0] & 0xff) >> rightShifts));
	   for (int i = 1; i < resultBytes.length; i++) {
	      byte tmp = resultBytes[i];
	      resultBytes[i] = (byte) (((resultBytes[i] & 0xff) >> rightShifts) | ((previousByte & 0xff) << leftShifts));
	      previousByte = tmp;
	   }
	   return resultBytes;
	}
	
	public static byte convertBinaryStringToByte(String input){
	     int tmpInt = Integer.parseInt(input,2);
	     byte tmpByte = (byte) tmpInt;
	     return tmpByte;
	}
	
	public static byte[] convertBinaryStringToBytes(String input){
		input = complementString(input);
		int numOfBytes = input.length() / 8;
		byte[] bytes = new byte[numOfBytes];
		for(int i = 0; i < numOfBytes; ++i)
		{
			bytes[i] = convertBinaryStringToByte(input.substring(8 * i, 8 * i + 8));
		}
		return bytes;
	}
	
	public static String complementString(String input){
		int remaining = input.length() % 8;
		if(remaining == 0)
			return input;
		for(int i = 0; i < 8 - remaining; i++)
			input += "0";
		return input;
	}
	
	public static String convertByteToBinaryString(byte b){
		StringBuilder sb = new StringBuilder(Byte.SIZE);
	    for( int i = 0; i < Byte.SIZE; i++ )
	        sb.append((b << i % Byte.SIZE & 0x80) == 0 ? '0' : '1');
	    return sb.toString();
	}
	
	public static String convertBytesToBinaryString(byte[] bytes){
		StringBuilder sb = new StringBuilder(bytes.length * Byte.SIZE);
	    for( int i = 0; i < Byte.SIZE * bytes.length; i++ )
	        sb.append((bytes[i / Byte.SIZE] << i % Byte.SIZE & 0x80) == 0 ? '0' : '1');
	    return sb.toString();
	}
	
	public static String convertBytesToBinaryStringKmer(byte[] bytes, int k){
		StringBuilder sb = new StringBuilder(bytes.length * Byte.SIZE);
	    for( int i = 0; i < Byte.SIZE * bytes.length; i++ )
	        sb.append((bytes[i / Byte.SIZE] << i % Byte.SIZE & 0x80) == 0 ? '0' : '1');
	    return sb.toString().substring(0,2*k);
	}
	
	public static byte[] addLastTwoBits(byte[] bytes, int lengthOfChainVertex, String newBits){
		String originalBytes = convertBytesToBinaryString(bytes).substring(0,2*lengthOfChainVertex);
		originalBytes += newBits;
		return convertBinaryStringToBytes(originalBytes);
	}
	
	public static String getLastTwoBits(byte[] bytes, int k){
		String sb = convertBytesToBinaryString(bytes);
		int num = k / 4;
		int extraBit = k % 4;
		if(extraBit >= 1)
			return sb.substring(8*num + 2*(extraBit-1),8*num + 2*extraBit);
		else
			return sb.substring(8*num - 2, 8*num);
	}
}
