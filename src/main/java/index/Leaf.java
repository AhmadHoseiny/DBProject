package index;

import helper_classes.GenericComparator;
import helper_classes.ReadConfigFile;

import java.io.IOException;
import java.util.Vector;

public class Leaf extends Node {

    Vector<Vector<Comparable>> keyDataVector;
    Vector<Vector<Integer>> pageIndexVector;
    Vector<Vector<Integer>> rowIndexVector;
    Vector<Boolean> hasData;

    public Leaf() throws IOException {
        this.keyDataVector = new Vector<>();
        int maximumEntriesInOctreeNode = ReadConfigFile.getMaximumEntriesInOctreeNode();
        this.hasData = new Vector<>();
        for (int i = 0; i < maximumEntriesInOctreeNode; i++) {
            this.hasData.add(false);
        }
        this.pageIndexVector = new Vector<>();
        this.rowIndexVector = new Vector<>();
    }

    public static boolean isDuplicate(Vector<Comparable> v1, Vector<Comparable> v2) {
        for (int i = 0; i < v1.size(); i++) {
            if (GenericComparator.compare(v1.get(i), v2.get(i)) != 0)
                return false;
        }
        return true;
    }

    //returns true if the keyData is inserted successfully, false otherwise(needs splitting)
    public boolean insertData(Vector<Comparable> keyData, int pageIndex, int rowIndex) throws IOException {
        for (int i = 0; i < keyDataVector.size(); i++) {
            if (isDuplicate(keyDataVector.get(i), keyData)) {
                addPointerToTable(pageIndex, rowIndex, i);
                return true;
            }
        }
//        System.out.println("keyDataVector.size() = " + keyDataVector.size() + " maximumEntriesInOctreeNode = " + ReadConfigFile.getMaximumEntriesInOctreeNode());
//        System.out.println(keyData);
        int maximumEntriesInOctreeNode = ReadConfigFile.getMaximumEntriesInOctreeNode();
        if (keyDataVector.size() < maximumEntriesInOctreeNode) {
            keyDataVector.add(keyData);
            addPointerToTable(pageIndex, rowIndex, keyDataVector.size() - 1);
            return true;
        } else {
            return false;
        }
    }

    public void addPointerToTable(int pageIndex, int rowIndex, int keyDataIndexInNode) {
        if (!hasData.get(keyDataIndexInNode)) {
            pageIndexVector.add(new Vector<>());
            rowIndexVector.add(new Vector<>());
            hasData.set(keyDataIndexInNode, true);
        }
        this.pageIndexVector.get(keyDataIndexInNode).add(pageIndex);
        this.rowIndexVector.get(keyDataIndexInNode).add(rowIndex);
    }

    public void updatePointer(Vector<Comparable> keyData, int oldPageIndex, int oldRowIndex, int newPageIndex, int newRowIndex) {
        for (int i = 0; i < keyDataVector.size(); i++) {
            if (isDuplicate(keyDataVector.get(i), keyData)) {
                for (int j = 0; j < pageIndexVector.get(i).size(); j++) {
                    if (pageIndexVector.get(i).get(j) == oldPageIndex && rowIndexVector.get(i).get(j) == oldRowIndex) {
                        pageIndexVector.get(i).set(j, newPageIndex);
                        rowIndexVector.get(i).set(j, newRowIndex);
                        return;
                    }
                }
            }
        }
    }

    public String toString() {
        return super.toString() + " " + keyDataVector.toString() + " " + pageIndexVector.toString() + " " + rowIndexVector.toString() + "}";
    }

    public void deleteData(Vector<Comparable> keyData, int pageIndex, int rowIndex) {
        for (int i = 0; i < keyDataVector.size(); i++) {
            if (isDuplicate(keyDataVector.get(i), keyData)) {
                for (int j = 0; j < pageIndexVector.get(i).size(); j++) {
                    if (Integer.compare(pageIndexVector.get(i).get(j), pageIndex) == 0 &&
                            Integer.compare(rowIndexVector.get(i).get(j), rowIndex) == 0) {
                        pageIndexVector.get(i).remove(j);
                        rowIndexVector.get(i).remove(j);
                        if (pageIndexVector.get(i).size() == 0) {
                            pageIndexVector.remove(i);
                            rowIndexVector.remove(i);
                            keyDataVector.remove(i);
                            hasData.remove(i);
                            hasData.add(false);
                        }
                        return;
                    }
                }
            }
        }
    }

    public void decrementPageIndicesLargerThanInput(int pageIndex) {
        for (Vector<Integer> v : pageIndexVector) {
            for (int i = 0; i < v.size(); i++) {
                if (Integer.compare(v.get(i), pageIndex) > 0) {
                    v.set(i, v.get(i) - 1);
                }
            }
        }
    }

}
