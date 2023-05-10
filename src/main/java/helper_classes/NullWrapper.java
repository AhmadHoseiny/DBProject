package helper_classes;

import java.io.Serializable;

public class NullWrapper<T> implements Comparable<Comparable>, Serializable {


    public NullWrapper() {
    }

    @Override
    public int compareTo(Comparable o) {
        return -1;
    }

    @Override
    public String toString() {
        return "Null Wrapper";
    }
}
