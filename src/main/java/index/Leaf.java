package index;

import helper_classes.ReadConfigFile;

import java.io.IOException;
import java.util.Vector;

public class Leaf extends Node {

    Vector<Vector<Comparable>> keyDataVector;
    Vector<Vector<Integer>> pageIndexVector;
    Vector<Vector<Integer>> rowIndexVector;
    Vector<Boolean> hasData;

    public Leaf () throws IOException {
        this.keyDataVector = new Vector<>();
        int maximumEntriesInOctreeNode = ReadConfigFile.getMaximumEntriesInOctreeNode();
        for(int i=0 ; i<maximumEntriesInOctreeNode ; i++){
            this.hasData.add(false);
        }
    }

    //returns true if the keyData is inserted successfully, false otherwise(needs splitting)
    public boolean insertData(Vector<Comparable> keyData, int pageIndex, int rowIndex) throws IOException {
        for(int i=0 ; i<keyDataVector.size() ; i++){
            if(isDuplicate(keyDataVector.get(i), keyData)){
                addPointerToTable(pageIndex, rowIndex, i);
                return true;
            }
        }
        int maximumEntriesInOctreeNode = ReadConfigFile.getMaximumEntriesInOctreeNode();
        if(keyDataVector.size() < maximumEntriesInOctreeNode){
            keyDataVector.add(keyData);
            addPointerToTable(pageIndex, rowIndex, keyDataVector.size()-1);
            return true;
        }
        else{
            return false;
        }
    }
    public static boolean isDuplicate(Vector<Comparable> v1, Vector<Comparable> v2){
        for(int i=0 ; i<v1.size() ; i++){
            if(v1.get(i).compareTo(v2.get(i)) != 0)
                return false;
        }
        return true;
    }
    public void addPointerToTable(int pageIndex, int rowIndex, int keyDataIndexInNode){
        if(!hasData.get(keyDataIndexInNode)){
            pageIndexVector.add(new Vector<>());
            rowIndexVector.add(new Vector<>());
            hasData.set(keyDataIndexInNode, true);
        }
        this.pageIndexVector.get(keyDataIndexInNode).add(pageIndex);
        this.rowIndexVector.get(keyDataIndexInNode).add(rowIndex);
    }
}
