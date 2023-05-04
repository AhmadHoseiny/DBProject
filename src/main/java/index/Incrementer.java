package index;

import java.util.Date;

public class Incrementer {
    public static Comparable increment( Comparable x) {
        if (x instanceof Integer)
            return (Integer) x + 1;
        else if (x instanceof Double)
            return (Double) x + 1;
        else if (x instanceof String)
            return ((String) x).charAt(((String) x).length() - 1) + 1 + "";
        else if (x instanceof Date)
            return new Date(((Date) x).getTime() + 1);
        else
            return null;
    }
}
