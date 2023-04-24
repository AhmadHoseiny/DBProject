package helper_classes;

import java.text.*;
import java.util.*;

public class main {
    public static void main(String[] args) throws ParseException {
        String s = "omar";
        String t = "omar";
        char le[] = s.toCharArray();
        char ri[] = t.toCharArray();
        int diffLen = ri.length - le.length;
        int incLen = (1+diffLen)>>1;
        char mid[] = new char [le.length + incLen];
        for(int j=0 ; j<mid.length ; j++){
            char leCh;
            if(j>=le.length){
                leCh = 'A';
            }
            else{
                leCh = le[j];
            }
            mid[j] = (char)( ((int)leCh + (int)ri[j])>>1);
        }
        System.out.println(new String(mid));
    }
}
