package index;

import java.util.Vector;

public class Leaf extends Node {

    Vector<Comparable> keyData;
    Vector<Integer> pageIndex;
    Vector<Integer> rowIndex;
    boolean hasData;

    public Leaf () {
        this.hasData = false;
    }

    public void insertData(Vector<Comparable> keyData, int pageIndex, int rowIndex){
        if(!this.hasData){
            this.keyData = new Vector<>();
            for(Comparable x : keyData)
                this.keyData.add(x);
            this.hasData = true;
        }
        this.addPointerToTable(pageIndex, rowIndex);

    }
    public void addPointerToTable(int pageIndex, int rowIndex) {
        if(this.pageIndex == null){
            this.pageIndex = new Vector();
        }
        if(this.rowIndex == null){
            this.rowIndex = new Vector();
        }
        this.pageIndex.add(pageIndex);
        this.rowIndex.add(rowIndex);
    }
}
