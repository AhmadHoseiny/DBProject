package helper_classes;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class GenericComparator {

    public static int compare(Comparable arg1, Comparable arg2) {
        if (arg1 instanceof NullWrapper && arg2 instanceof NullWrapper) {
            return arg1.compareTo(arg2);
        }
        if (arg1 instanceof NullWrapper) {
            return -1;
        }
        if (arg2 instanceof NullWrapper) {
            return 1;
        }
        if (arg1 instanceof Date && arg2 instanceof Date) {
            if (((Date) arg1).getTime() < ((Date) arg2).getTime()) {
                return -1;
            }
            else if (((Date) arg1).getTime() == ((Date) arg2).getTime()) {
                return 0;
            }
            else {
                return 1;
            }
        }
        return arg1.compareTo(arg2);
    }
}
