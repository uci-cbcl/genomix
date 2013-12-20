package edu.uci.ics.genomix.data.types;

import org.junit.Test;

public class GeneralTest {

    @Test
    public void TestByte() {
        byte a = -1;
        int b = (a & 0xff) << 8;
        System.out.println(b);
        System.out.println((byte) ((b >> 8) & 0xff));
        cA ca = new cA();
    }
    
    class cA {
        int x = 42;
        public cA(){
            System.out.println("before set: x = " + x);
            x = 20;
        }
    }
}
