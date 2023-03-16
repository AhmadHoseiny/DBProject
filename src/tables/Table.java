package tables;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import exceptions.DBAppException;
import helper_classes.*;

public class Table implements Serializable {
    private String tableName;
    private String clusteringKey;
    private Vector<String> colNames;
    private Vector<String> colTypes;
    private Vector<String> colMin;
    private Vector<String> colMax;
    private Vector<Page> table;

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


    public Table() {

    }

    public void createTable(String strTableName,
                            String strClusteringKeyColumn,
                            Hashtable<String,String> htblColNameType,
                            Hashtable<String,String> htblColNameMin,
                            Hashtable<String,String> htblColNameMax )
            throws DBAppException, IOException {
        Table t = new Table(strTableName, strClusteringKeyColumn, htblColNameType, htblColNameMin, htblColNameMax);
        Serializer.serializeTable(t, strTableName);
    }

    public void insertIntoTable(String strTableName,
                                Hashtable<String,Object> htblColNameValue)
            throws DBAppException, IOException, ClassNotFoundException, ParseException {
        Table t = Serializer.deserializeTable(strTableName);
        t.insertTuple(htblColNameValue);
        Serializer.serializeTable(t, strTableName);
    }

    public boolean isValidTuple(Hashtable<String, Object> htblColNameValue) throws ParseException {

        boolean isValid = true;
        SimpleDateFormat formatter = new SimpleDateFormat("YYYY-MM-DD", Locale.ENGLISH);

        for(int i = 0; i < colNames.size(); i++){

//            isValid &= ((Comparable)htblColNameValue.get(colNames.get(i))).compareTo(colMin.get(i))>=0 ;
//            isValid &= ((Comparable)htblColNameValue.get(colNames.get(i))).compareTo(colMax.get(i))<=0 ;

            switch (colTypes.get(i)) {

                case "java.lang.Integer" : isValid &= ((Comparable)htblColNameValue.get(colNames.get(i))).compareTo(Integer.parseInt(colMin.get(i)))>=0 ; break;
                case "java.lang.String" : isValid &= ((Comparable)htblColNameValue.get(colNames.get(i))).compareTo(colMin.get(i))>=0 ; break;
                case "java.lang.Double" :
                case "java.lang.double" : isValid &= ((Comparable)htblColNameValue.get(colNames.get(i))).compareTo(Double.parseDouble(colMin.get(i)))>=0 ; break;
                case "java.util.Date" : isValid &= ((Comparable)htblColNameValue.get(colNames.get(i))).compareTo(formatter.parse(colMin.get(i)))>=0 ; break;
                default: isValid &= false;

            }

//            switch (colTypes.get(i)) {
//
//                case "java.lang.Integer" : isValid &= ((Comparable)htblColNameValue.get(colNames.get(i))).compareTo(Integer.parseInt(colMax.get(i)))>=0 ; break;
//                case "java.lang.String" : isValid &= ((Comparable)htblColNameValue.get(colNames.get(i))).compareTo(colMax.get(i))>=0 ; break;
//                case "java.lang.Double" :
//                case "java.lang.double" : isValid &= ((Comparable)htblColNameValue.get(colNames.get(i))).compareTo(Double.parseDouble(colMax.get(i)))>=0 ; break;
//                case "java.util.Date" : isValid &= ((Comparable)htblColNameValue.get(colNames.get(i))).compareTo(formatter.parse(colMax.get(i)))>=0 ; break;
//                default: isValid &= false;
//
//            }

            switch (colTypes.get(i)) {

                case "java.lang.Integer" : isValid &= htblColNameValue.get(colNames.get(i)) instanceof Integer; break;
                case "java.lang.String" : isValid &= htblColNameValue.get(colNames.get(i)) instanceof String; break;
                case "java.lang.Double" :
                case "java.lang.double" : isValid &= htblColNameValue.get(colNames.get(i)) instanceof Double; break;
                case "java.util.Date" : isValid &= htblColNameValue.get(colNames.get(i)) instanceof Date; break;
                default: isValid &= false;

            }

        }

        return isValid;

    }

    public void insertTuple(Hashtable<String, Object> htblColNameValue)
            throws DBAppException, IOException, ClassNotFoundException, ParseException {

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