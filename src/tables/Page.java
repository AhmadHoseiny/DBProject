package tables;

import java.io.*;
import java.util.*;

import exceptions.DBAppException;
import helper_classes.*;
public class Page implements Serializable{
    private Vector<Vector<Object>> page;

    public Vector<Vector<Object>> getPage() {
        return page;
    }

    public Page() {
        page = new Vector<>();
    }

//    returns last tuple if page is full, null otherwise

    public  Vector<Object> getInFormOfTuple(Vector<String> colNames,
                                                  Hashtable<String, Object> htblColNameValue){
        Vector<Object> tuple = new Vector<>();
        for(String colName : colNames){
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

    public Integer binarySearch (Comparable strClusteringVal) {
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

    public int getIndexToInsertAt ( Comparable strClusteringVal ) throws DBAppException {
        int index = binarySearch(strClusteringVal);
        if (index == -1)
            return 0;
        Comparable cur = (Comparable) this.getPage().get(index).get(0);
        if (cur.compareTo(strClusteringVal) == 0)
            throw new DBAppException("Duplicate Clustering Key");

        return index + 1;
    }

    public int getTupleIndex ( Comparable strClusteringVal ) throws DBAppException {
        int index = binarySearch(strClusteringVal);
        if (index == -1)
            throw new DBAppException("The input clustering key does not exist");

        Comparable cur = (Comparable) this.getPage().get(index).get(0);
        if (cur.compareTo(strClusteringVal) != 0)
            throw new DBAppException("The input clustering key does not exist");
        return index;
    }

    public Vector<Object> insertAtBeginning(Vector<Object> tuple) throws IOException {
        page.insertElementAt(tuple, 0);
        int maxSize = ReadConfigFile.getMaximumRowsCountInTablePage();
        if(page.size()==maxSize+1){
            return this.getPage().remove(this.getPage().size()-1);
        }
        return null;
    }

    public void updateTuple(Comparable strClusteringVal, Vector<String> colNames, Hashtable<String, Object> htblColNameValue) throws DBAppException {

        int index = getTupleIndex(strClusteringVal);
        Vector<Object> tupleToBeUpdated = this.getPage().get(index);
        for (int i = 0; i < colNames.size(); i++)
            if (htblColNameValue.containsKey(colNames.get(i)))
                tupleToBeUpdated.set(i, htblColNameValue.get(colNames.get(i)));

    }

    //returns false when page is empty after deletion
    public boolean deleteSingleTuple(Comparable strClusteringVal, Vector<String> colNames, Hashtable<String, Object> htblColNameValue) throws DBAppException {
        int index = getTupleIndex(strClusteringVal);

        boolean valueExists = true;
        for(int i = 0; i < colNames.size(); i++) {
            String colName = colNames.get(i);
            Comparable value = (Comparable) htblColNameValue.get(colName);
            if(value != null)
                valueExists &= value.equals(this.getPage().get(index).get(i));
        }

        if(valueExists)
            this.getPage().remove(index);
        else
            throw new DBAppException("The input criteria to be deleted does not exist");

        if(this.getPage().size() == 0)
            return false;

        return true;
    }

    //returns false when page is empty after deletion
    public boolean deleteAllMatchingTuples(Vector<String> colNames, Hashtable<String, Object> htblColNameValue){

        for(int i = 0; i < this.getPage().size(); i++){
            Vector<Object> tuple = this.getPage().get(i);
            boolean valueExists = true;
            for(int j = 0; j < colNames.size(); j++) {
                String colName = colNames.get(j);
                Comparable value = (Comparable) htblColNameValue.get(colName);
                if(value != null)
                    valueExists &= value.equals(tuple.get(j));
            }
            if(valueExists)
                this.getPage().remove(i--);
        }

        if(this.getPage().size() == 0)
            return false;

        return true;

    }

}



