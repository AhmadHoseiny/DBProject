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

    //returns true if inserting is successful without exceeding max no. of rows in a page
//    public boolean insert(Vector<String> colNames , Hashtable<String, Object> htblColNameValue)
//            throws IOException, DBAppException {
//        int maxSize = ReadConfigFile.getMaximumRowsCountInTablePage();
//        if(page.size()>=maxSize)
//            return false;
//        Vector<Object> tuple = new Vector<>();
//        for(String colName : colNames){
//            tuple.add(htblColNameValue.get(colName));
//        }
//        page.add(tuple);
//        return true;
//    }

    //returns false if after insertion, the maxSize is exceeded
    //return value: "-1" --> didn't insert (try an earlier page)
    //              "0" --> inserted successfully (without page overflow)
    //              "1" --> inserted successfully (with page overflow)
    public int insertToSorted(Vector<String> colNames, Hashtable<String, Object> htblColNameValue,
                                  boolean forceInsert) throws IOException, DBAppException {

        Vector<Object> tuple = new Vector<>();
        for(String colName : colNames){
            tuple.add(htblColNameValue.get(colName));
        }

        String strClusteringKey = colNames.get(0);
        Comparable strClusteringVal = (Comparable) htblColNameValue.get(strClusteringKey);

        int s = page.size();
        boolean inserted = false ;
        for(int i=s-1 ; i>=0 ; i--){
            Vector<Object> curTuple = page.get(i);
            Comparable curKey = (Comparable) curTuple.get(0);
            if(curKey.compareTo(strClusteringVal)==0){
                throw new DBAppException("Duplicate Clustering Key");
            }
            if(curKey.compareTo(strClusteringVal)<0){
                page.insertElementAt(tuple, i+1);
                inserted = true;
                break;
            }
        }
        if(!inserted && forceInsert){
            page.insertElementAt(tuple, 0);
            inserted = true;
        }
        if(!inserted){
            return -1;
        }
        else{
            int maxSize = ReadConfigFile.getMaximumRowsCountInTablePage();
            return (page.size() == maxSize+1)? 1 : 0;
        }
    }

    public Vector<Object> insertAtBeginning(Vector<Object> tuple) throws IOException {
        page.insertElementAt(tuple, 0);
        int maxSize = ReadConfigFile.getMaximumRowsCountInTablePage();
        if(page.size()==maxSize+1){
            return page.remove(page.size()-1);
        }
        return null;
    }

    public Integer getTupleIndex (Comparable strClusteringVal) {
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

        Comparable cur = (Comparable) this.getPage().get(hi).get(0);
        return (cur.compareTo(strClusteringVal) == 0)?hi:null;
    }

    public void updateTuple(String strClusteringVal, Vector<String> colNames, Hashtable<String, Object> htblColNameValue) throws DBAppException {

        Integer index = getTupleIndex(strClusteringVal);
        if (index == null)
            throw new DBAppException("The input clustering key to be updated does not exist");

        Vector<Object> tupleToBeUpdated = this.getPage().get(index);
        for (int i = 0; i < colNames.size(); i++)
            if (htblColNameValue.containsKey(colNames.get(i)))
                tupleToBeUpdated.set(i, htblColNameValue.get(colNames.get(i)));

    }

    //implement a method that deletes a tuple from the page after deserializing it and serializing it again
    public void deleteTuple(String strClusteringVal) throws DBAppException {
        Integer index = getTupleIndex(strClusteringVal);
        if (index == null)
            throw new DBAppException("The input clustering key to be deleted does not exist");

        this.getPage().remove(index.intValue());
    }

    //returns false when page is empty after deletion
    public boolean deleteSingleTuple(Comparable strClusteringVal, Vector<String> colNames, Hashtable<String, Object> htblColNameValue) throws DBAppException {
        Integer index = getTupleIndex(strClusteringVal);
        if (index == null)
            throw new DBAppException("The input clustering key to be deleted does not exist");
        boolean valueExists = true;
        for(int i = 0; i < colNames.size(); i++) {
            String colName = colNames.get(i);
            Comparable value = (Comparable) htblColNameValue.get(colName);
            if(value != null)
                valueExists &= value.equals(this.getPage().get(index).get(i));
        }

        if(valueExists)
            this.getPage().remove(index.intValue());
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


////        Vector<String> colNames = new Vector<>();
//        for(String colName : htblColNameValue.keySet()){
//            colNames.add(colName);
//        }
//        Collections.sort(colNames);
//        Integer index = getTupleIndex(strClusteringVal);
//        if (index == null)
//            throw new DBAppException("The input clustering key to be deleted does not exist");
//
//        Vector<Vector<Object>> tuplesToBeDeleted = new Vector<>();
//        for(int i = index; i < this.getPage().size(); i++){
//            Vector<Object> tuple = this.getPage().get(i);
//            boolean valueExists = true;
//            for(int j = 0; j < colNames.size(); j++) {
//                String colName = colNames.get(j);
//                Comparable value = (Comparable) htblColNameValue.get(colName);
//                if(value != null)
//                    valueExists &= value.equals(tuple.get(j));
//            }
//            if(valueExists)
//                tuplesToBeDeleted.add(tuple);
//        }
//
//        for(Vector<Object> tuple : tuplesToBeDeleted){
//            this.getPage().remove(tuple);
//        }



    }

}



