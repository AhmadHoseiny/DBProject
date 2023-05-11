package helper_classes;

import java.io.Serializable;

public class NullWrapper implements Comparable<NullWrapper>, Serializable {


    @Override
    public String toString() {
        return "Null Wrapper";
    }

    @Override
    public int compareTo(NullWrapper o) {
        return 0;
    }
}
