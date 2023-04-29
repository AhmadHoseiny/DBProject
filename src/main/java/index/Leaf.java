package index;

import java.util.Vector;

public class Leaf extends Node {
    Comparable x;
    Comparable y;
    Comparable z;
    Vector<Integer> pageIndex;
    Vector<Integer> rowIndex;
    boolean hasData;

    public Leaf (Comparable x, Comparable y, Comparable z, int pageIndex, int rowIndex) {
        this.x = x;
        this.y = y;
        this.z = z;
        this.pageIndex = new Vector<>();
        this.rowIndex = new Vector<>();
        this.add(pageIndex, rowIndex);
        this.hasData = true;
    }

    public Leaf () {
        this.hasData = false;
    }


    public void add(int pageIndex, int rowIndex) {
        this.pageIndex.add(pageIndex);
        this.rowIndex.add(rowIndex);
    }
}
