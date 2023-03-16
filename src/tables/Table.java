package tables;

import java.io.*;
import java.util.*;

import exceptions.DBAppException;
import helper_classes.*;

public class Table implements Serializable {
    final String tableName;
    final String clusteringKey;
    Vector<String> colNames;
    Vector<String> colTypes;
    Vector<String> colMin;
    Vector<String> colMax;
    Vector<Page> table;

    public String getTableName() {
        return tableName;
    }

    public String getClusteringKey() {
        return clusteringKey;
    }

    public Vector<String> getColNames() {
        return colNames;
    }

    public Vector<String> getColTypes() {
        return colTypes;
    }

    public Vector<String> getColMin() {
        return colMin;
    }

    public Vector<String> getColMax() {
        return colMax;
    }

    public Vector<Page> getTable() {
        return table;
    }

    public Table(String strTableName,
                 String strClusteringKeyColumn,
                 Hashtable<String, String> htblColNameType,
                 Hashtable<String, String> htblColNameMin,
                 Hashtable<String, String> htblColNameMax) throws DBAppException {
        if(htblColNameType.size() != htblColNameMin.size() ||
                htblColNameMin.size() !=  htblColNameMax.size() ||
                htblColNameType.size() != htblColNameMax.size())
            throw new DBAppException("The input data is inconsistent");
        this.tableName = strTableName;
        this.clusteringKey = strClusteringKeyColumn;
        this.colNames = new Vector<>();
        this.colTypes = new Vector<>();
        this.colMin = new Vector<>();
        this.colMax = new Vector<>();
        colNames.add(this.clusteringKey);
        colTypes.add(htblColNameType.get(this.clusteringKey));
        for(Map.Entry<String, String> e : htblColNameType.entrySet()){
            if(e.getKey().equals(this.clusteringKey))
                continue;
            colNames.add(e.getKey());
            colTypes.add(e.getValue());
        }
        for(String colName : colNames) {
            colMin.add(htblColNameMin.get(colName));
            colMax.add(htblColNameMax.get(colName));
        }
        this.table = new Vector<>();

        new File("Serialized Files/" + this.tableName).mkdirs();
    }

    public boolean isValidTuple(Hashtable<String, Object> htblColNameValue){
        Vector<Comparable> tuple = new Vector<>();
        for(String colName : colNames){
            tuple.add((Comparable)htblColNameValue.get(colName));
        }
        boolean isValid = true;
        for(int i=0 ; i<tuple.size() ; i++){
            isValid &= tuple.get(i).compareTo(colMin.get(i))>=0 ;
            isValid &= tuple.get(i).compareTo(colMax.get(i))<=0 ;
        }
        return isValid;
    }
    public void insertTuple(Hashtable<String, Object> htblColNameValue)
            throws DBAppException, IOException, ClassNotFoundException {
        //Don't forget to check between min & max
        if(!isValidTuple(htblColNameValue))
            throw new DBAppException("The values inserted do not respect the constraints");
        if(this.table.isEmpty()){
            Page page = new Page();
            this.table.add(page);
            page.insertToSorted(colNames, htblColNameValue, true);
            Serializer.serializePage(page, this.getTableName(), 0);
            return;
        }

        File folder = new File("Serialized Files/" + tableName);
        int fileCount = folder.listFiles().length;

        int i;
        Vector<Object> lastTuple = null;
        for(i=fileCount-1 ; i>=0 ; i--){
            Page curPage = Serializer.deserializePage(this.getTableName(), i);
            int retVal = curPage.insertToSorted(colNames, htblColNameValue, (i==0)?true:false);
            if(retVal == 0){
                Serializer.serializePage(curPage, this.getTableName(), i);
                return;
            }
            if(retVal == 1){
                lastTuple = curPage.getPage().remove(curPage.getPage().size()-1);
                Serializer.serializePage(curPage, this.getTableName(), i);
                break;
            }
        }
        for(int j=i+1 ; j<fileCount ; j++){
            Page page = Serializer.deserializePage(this.getTableName(), j);
            lastTuple = page.insertAtBeginning(lastTuple);
            Serializer.serializePage(page, this.getTableName(), j);
        }
        if(lastTuple != null){
            Page page = new Page();
            this.table.add(page);
            Hashtable<String, Object> ht = new Hashtable<>();
            for(int j=0 ; j<colNames.size() ; j++){
                ht.put(colNames.get(j), lastTuple.get(j));
            }
            page.insertToSorted(colNames, ht, true);
            Serializer.serializePage(page, this.getTableName(), fileCount);
        }



    }


}
