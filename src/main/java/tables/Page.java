package tables;

import exceptions.DBAppException;
import helper_classes.ReadConfigFile;
import helper_classes.NullWrapper;
import index.Octree;

import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Vector;

public class Page implements Serializable {
    private Vector<Vector<Object>> page;

    public Page() {
        page = new Vector<>();
    }

    public Vector<Vector<Object>> getPage() {
        return page;
    }

//    returns last tuple if page is full, null otherwise

    public Vector<Object> getInFormOfTuple(Vector<String> colNames,
                                           Hashtable<String, Object> htblColNameValue) {
        Vector<Object> tuple = new Vector<>();
        for (String colName : colNames) {
            if (!htblColNameValue.containsKey(colName))
                tuple.add(new NullWrapper());
            else
                tuple.add(htblColNameValue.get(colName));
        }
        return tuple;
    }

    public Vector<Object> insertToSorted(Vector<String> colNames, Hashtable<String, Object> htblColNameValue) throws IOException, DBAppException {

        Vector<Object> tuple = getInFormOfTuple(colNames, htblColNameValue);

        String strClusteringKey = colNames.get(0);
        Comparable strClusteringVal = (Comparable) htblColNameValue.get(strClusteringKey);

        int index = getIndexToInsertAt(strClusteringVal);
        page.insertElementAt(tuple, index);

        int maxSize = ReadConfigFile.getMaximumRowsCountInTablePage();
        if (this.getPage().size() == maxSize + 1) {
            return this.getPage().remove(this.getPage().size() - 1);
        }
        return null;
    }

    public void insertTuple(Vector<String> colNames, Hashtable<String, Object> htblColNameValue) {

        Vector<Object> tuple = getInFormOfTuple(colNames, htblColNameValue);

        this.getPage().add(tuple);

    }

    public Integer binarySearch(Comparable strClusteringVal) {
        int lo = 0;
        int hi = this.getPage().size() - 1;

        while (lo <= hi) {
            int mid = lo + (hi - lo >> 1);
            Comparable cur = (Comparable) this.getPage().get(mid).get(0);
            if (cur.compareTo(strClusteringVal) > 0)
                hi = mid - 1;
            else
                lo = mid + 1;
        }

        return hi;
    }

    public int getIndexToInsertAt(Comparable strClusteringVal) throws DBAppException {
        int index = binarySearch(strClusteringVal);
        if (index == -1)
            return 0;
        Comparable cur = (Comparable) this.getPage().get(index).get(0);
        if (cur.compareTo(strClusteringVal) == 0) {
//            System.out.println(cur + " " + strClusteringVal);
            throw new DBAppException("Duplicate Clustering Key");
        }

        return index + 1;
    }

    public int getTupleIndex(Comparable strClusteringVal) throws DBAppException {
        int index = binarySearch(strClusteringVal);
        if (index == -1)
            throw new DBAppException("The input clustering key does not exist");

        Comparable cur = (Comparable) this.getPage().get(index).get(0);
        if (cur.compareTo(strClusteringVal) != 0)
            throw new DBAppException("The input clustering key does not exist, cur: " + cur + " strClusteringVal: " + strClusteringVal);
        return index;
    }

    public Vector<Object> insertAtBeginning(Vector<Object> tuple) throws IOException {
        page.insertElementAt(tuple, 0);
        int maxSize = ReadConfigFile.getMaximumRowsCountInTablePage();
        if (page.size() == maxSize + 1) {
            return this.getPage().remove(this.getPage().size() - 1);
        }
        return null;
    }

    public void updateTuple(Comparable strClusteringVal, Vector<String> colNames, Hashtable<String, Object> htblColNameValue) throws DBAppException {
        try {
            int index = getTupleIndex(strClusteringVal);
            Vector<Object> tupleToBeUpdated = this.getPage().get(index);
            for (int i = 0; i < colNames.size(); i++)
                if (htblColNameValue.containsKey(colNames.get(i)))
                    tupleToBeUpdated.set(i, htblColNameValue.get(colNames.get(i)));
        } catch (DBAppException e) {
            if (!e.getMessage().equals("The input clustering key does not exist")) {
                throw new DBAppException(e.getMessage());
            }
        }

    }

    //returns false when page is empty after deletion
    public boolean deleteSingleTuple(Comparable strClusteringVal, Vector<String> colNames, Hashtable<String, Object> htblColNameValue) throws DBAppException {
        try {
            int index = getTupleIndex(strClusteringVal);

            boolean valueExists = true;
            for (int i = 0; i < colNames.size(); i++) {
                String colName = colNames.get(i);
                Comparable value = (Comparable) htblColNameValue.get(colName);
                if (value != null)
                    valueExists &= value.equals(this.getPage().get(index).get(i));
            }

            if (valueExists)
                this.getPage().remove(index);
            else
                throw new DBAppException("The input criteria to be deleted does not exist");
        } catch (DBAppException e) {
            if (!e.getMessage().equals("The input criteria to be deleted does not exist") && !e.getMessage().equals("The input clustering key does not exist")) {
                throw new DBAppException(e.getMessage());
            }
        }

        if (this.getPage().size() == 0)
            return false;

        return true;
    }

    //returns false when page is empty after deletion
    public boolean deleteAllMatchingTuples(Vector<String> colNames,
                                           Hashtable<String, Object> htblColNameValue,
                                           int oldPageIndex, int newPageIndex,
                                           Table t, HashSet<Octree> deserializedOctrees) throws DBAppException, IOException, ParseException, ClassNotFoundException {

        Vector<Vector<Object>> newPage = new Vector<>();
        int cntDeleted = 0;
        HashMap<Integer, Integer> toBeUpdated = new HashMap<>(); //oldRowIndex --> newRowIndex

        for(int i=0 ; i<this.getPage().size() ; i++){
            Vector<Object> tuple = this.getPage().get(i);
            boolean valueExists = true;
            for (int j = 0; j < colNames.size(); j++) {
                String colName = colNames.get(j);
                Comparable value = (Comparable) htblColNameValue.get(colName);
                if (value != null) {
                    valueExists &= value.equals(tuple.get(j));
                }
            }
            //to be deleted
            if (valueExists){
                t.deleteInOctree(deserializedOctrees, tuple, oldPageIndex, i);
                cntDeleted++;
            }
            else{
                newPage.add(tuple);
                toBeUpdated.put(i, i-cntDeleted);
            }
        }

        for(int oldRowIndex : toBeUpdated.keySet()){
            int newRowIndex = toBeUpdated.get(oldRowIndex);
            Vector<Object> tuple = this.getPage().get(oldRowIndex);
            t.updatePointerInOctree(deserializedOctrees, tuple, oldPageIndex, oldRowIndex,
                    newPageIndex, newRowIndex);
        }

        this.page = newPage;
        if (this.getPage().size() == 0)
            return false;

        return true;


    }

}



