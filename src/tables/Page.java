package tables;

import java.io.*;
import java.util.*;

import exceptions.DBAppException;
import helper_classes.*;
public class Page implements Serializable{
    Vector<Vector<Object>> page;

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
}
