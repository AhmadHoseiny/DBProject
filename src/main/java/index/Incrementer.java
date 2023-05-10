package index;

import java.util.Date;

public class Incrementer {
    public static Comparable increment(Comparable x) {
        if (x instanceof Integer)
            return (Integer) x + 1;
        else if (x instanceof Double)
            return (Double) x + 1e-6;
        else if (x instanceof String) {

            char[] chars = ((String) x).toCharArray();

            for (int i = chars.length - 1; i >= 0; i--) {
                if (chars[i] != 'z') {
                    chars[i]++;
                    return new String(chars);
                }
            }
            return "!@#$%^&*(){}<>[]";
//            return ((String) x).substring(0, ((String) x).length() - 1) + "" + ((char) (((String) x).charAt(((String) x).length() - 1) + 1));
        }
        else if (x instanceof Date)
            return new Date(((Date) x).getTime() + 1);
        else
            return null;
    }
}
