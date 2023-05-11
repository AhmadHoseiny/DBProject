package helper_classes;

public class GenericComparator {
    public static int compare(Comparable arg1, Comparable arg2){
        if(arg1 instanceof NullWrapper && arg2 instanceof NullWrapper){
            return arg1.compareTo(arg2);
        }
        if(arg1 instanceof NullWrapper){
            return -1;
        }
        if(arg2 instanceof NullWrapper){
            return 1;
        }
        return arg1.compareTo(arg2);
    }
}
