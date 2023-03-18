package tables;

import java.io.*;
import java.text.ParseException;
import java.util.*;

import exceptions.DBAppException;
import helper_classes.*;

public class Table implements Serializable {
    private final String tableName;
    private final String clusteringKey;
    private Vector<String> colNames;
    private Vector<String> colTypes;
    private Vector<String> colMin;
    private Vector<String> colMax;
    private HashMap<Integer, Comparable> dataIndex;
    private transient Vector<Page> table;

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

    public void setTable(Vector<Page> t) { this.table = t; }

    public Table(String strTableName,
                 String strClusteringKeyColumn,
                 Hashtable<String, String> htblColNameType,
                 Hashtable<String, String> htblColNameMin,
                 Hashtable<String, String> htblColNameMax) throws DBAppException, IOException {

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
        this.dataIndex = new HashMap<>();

        colNames.add(this.clusteringKey);
        colTypes.add(htblColNameType.get(this.clusteringKey));

        for(Map.Entry<String, String> e : htblColNameType.entrySet()){

            if(e.getKey().equals(this.clusteringKey)) {
                continue;
            }
            colNames.add(e.getKey());
            colTypes.add(e.getValue());

        }

        for(String colName : colNames) {

            colMin.add(htblColNameMin.get(colName));
            colMax.add(htblColNameMax.get(colName));

        }

        this.table = new Vector<>();


//        Vector<Object> details = new Vector<>();
//        details.add(strClusteringKeyColumn);
//        details.add(htblColNameType);
//        details.add(htblColNameMin);
//        details.add(htblColNameMax);
//
//        FileOutputStream fileOut =
//                new FileOutputStream("Serialized Database/" +
//                        tableName + ".ser");
//        ObjectOutputStream out = new ObjectOutputStream(fileOut);
//        out.writeObject(details);
//        out.close();
//        fileOut.close();

    }


    public boolean isValidTuple(Hashtable<String, Object> htblColNameValue) {

//        for(int i = 0; i < colNames.size(); i++){
//
//            switch (colTypes.get(i)) {
//
//                case "java.lang.Integer" : isValid &= htblColNameValue.get(colNames.get(i)) instanceof Integer; break;
//                case "java.lang.String" : isValid &= htblColNameValue.get(colNames.get(i)) instanceof String; break;
//                case "java.lang.Double" :
//                case "java.lang.double" : isValid &= htblColNameValue.get(colNames.get(i)) instanceof Double; break;
//                case "java.util.Date" : isValid &= htblColNameValue.get(colNames.get(i)) instanceof Date; break;
//                default: isValid &= false;
//
//            }
//
//        }

//        Vector<Comparable> tuple = new Vector<>();
//        for(String colName : colNames){
//            tuple.add((Comparable)htblColNameValue.get(colName));
//        }

        boolean isValid = true;

        for(int i = 0; i < colNames.size(); i++){

//            isValid &= ((Comparable)htblColNameValue.get(colNames.get(i))).compareTo(colMin.get(i))>=0 ;
//            isValid &= ((Comparable)htblColNameValue.get(colNames.get(i))).compareTo(colMax.get(i))<=0 ;

            if (!htblColNameValue.containsKey(colNames.get(i)))
                continue;

            switch (colTypes.get(i)) {

                case "java.lang.Integer" : isValid &= ((Comparable)htblColNameValue.get(colNames.get(i))).compareTo(Integer.parseInt(colMin.get(i)))>=0 ; break;
                case "java.lang.String" : isValid &= ((Comparable)htblColNameValue.get(colNames.get(i))).compareTo(colMin.get(i))>=0 ; break;
                case "java.lang.Double" :
                case "java.lang.double" : isValid &= ((Comparable)htblColNameValue.get(colNames.get(i))).compareTo(Double.parseDouble(colMin.get(i)))>=0 ; break;
                case "java.util.Date" : isValid &= ((Comparable)htblColNameValue.get(colNames.get(i))).compareTo(Date.parse(colMin.get(i)))>=0 ; break;
                default: isValid &= false;

            }

//            switch (colTypes.get(i)) {
//
//                case "java.lang.Integer" : isValid &= ((Comparable)htblColNameValue.get(colNames.get(i))).compareTo(Integer.parseInt(colMax.get(i)))>=0 ; break;
//                case "java.lang.String" : isValid &= ((Comparable)htblColNameValue.get(colNames.get(i))).compareTo(colMax.get(i))>=0 ; break;
//                case "java.lang.Double" :
//                case "java.lang.double" : isValid &= ((Comparable)htblColNameValue.get(colNames.get(i))).compareTo(Double.parseDouble(colMax.get(i)))>=0 ; break;
//                case "java.util.Date" : isValid &= ((Comparable)htblColNameValue.get(colNames.get(i))).compareTo(Date.parse(colMax.get(i)))>=0 ; break;
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

    public int getPageIndex(Comparable clusteringKeyVal) {
        File folder = new File("Serialized Database/" + tableName);
        int fileCount = folder.listFiles().length;

        int lo = 0;
        int hi = fileCount - 1;

        while (lo <= hi) {
            int mid = lo + (hi - lo >> 1);
//            System.out.println(dataIndex.get(mid).getClass());
//            System.out.println(clusteringKeyVal.getClass());
            if (dataIndex.get(mid).compareTo(clusteringKeyVal) > 0) {
                hi = mid - 1;
            }
            else {
                lo = mid + 1;
            }
        }
        return hi;
    }

    public void insertTuple(Hashtable<String, Object> htblColNameValue)
            throws DBAppException, IOException, ClassNotFoundException {

        //Don't forget to check between min & max
        if(!isValidTuple(htblColNameValue))
            throw new DBAppException("The values inserted do not respect the constraints");

        if (htblColNameValue.get(colNames.get(0)) == null)
            throw new DBAppException("Tuple has no clustering key value");


        String directoryPath = "Serialized Database/" + this.tableName;
        File directory = new File(directoryPath);

        if(!directory.isDirectory())
            throw new DBAppException("This table does not exist!");

        if(directory.isDirectory() && directory.listFiles().length == 0){

            Page page = new Page();
            this.table.add(page);
            page.insertToSorted(colNames, htblColNameValue, true);
            dataIndex.put(0, (Comparable) htblColNameValue.get(clusteringKey));
            Serializer.serializePage(page, this.getTableName(), 0);
            return;

        }

        int ans = getPageIndex((Comparable) htblColNameValue.get(this.getClusteringKey()));

        Page curPage = Serializer.deserializePage(tableName, ans);
        Vector<Object> lastTuple = null;
        int retVal = curPage.insertToSorted(colNames, htblColNameValue, (ans==0)?true:false);

        if(retVal == 0){
            Serializer.serializePage(curPage, this.getTableName(), ans);
            return;
        }
        if(retVal == 1){
            lastTuple = curPage.getPage().remove(curPage.getPage().size()-1);
            Serializer.serializePage(curPage, this.getTableName(), ans);
        }

        //the next loop is for the case when we shift every tuple in all the pages below the one we are inserting in
        int j = 0;
        File folder = new File("Serialized Database/" + tableName);
        int fileCount = folder.listFiles().length;
        for(j=ans+1 ; j<fileCount ; j++){

            Page page = Serializer.deserializePage(this.getTableName(), j);
            dataIndex.put(j, (Comparable) lastTuple.get(0));
            lastTuple = page.insertAtBeginning(lastTuple);
            Serializer.serializePage(page, this.getTableName(), j);

        }

        if(lastTuple != null){

            Page page = new Page();
            this.table.add(page);
            Hashtable<String, Object> ht = new Hashtable<>();
            for(int k=0 ; k<colNames.size() ; k++){
                ht.put(colNames.get(k), lastTuple.get(k));
            }
            page.insertToSorted(colNames, ht, true);
            dataIndex.put(j, (Comparable) lastTuple.get(0));
            Serializer.serializePage(page, this.getTableName(), fileCount);

        }







//        int i;
//        Vector<Object> lastTuple = null;
//
//        for(i=fileCount-1 ; i>=0 ; i--){
//
//            Page curPage = Serializer.deserializePage(tableName, i);
//            int retVal = curPage.insertToSorted(colNames, htblColNameValue, (i==0)?true:false);
//
//            if(retVal == 0){
//                Serializer.serializePage(curPage, this.getTableName(), i);
//                return;
//            }
//            if(retVal == 1){
//                lastTuple = curPage.getPage().remove(curPage.getPage().size()-1);
//                Serializer.serializePage(curPage, this.getTableName(), i);
//                break;
//            }
//
//        }
//
//        for(int j=i+1 ; j<fileCount ; j++){
//
//            Page page = Serializer.deserializePage(this.getTableName(), j);
//            lastTuple = page.insertAtBeginning(lastTuple);
//            Serializer.serializePage(page, this.getTableName(), j);
//
//        }
//
//        if(lastTuple != null){
//
//            Page page = new Page();
//            this.table.add(page);
//            Hashtable<String, Object> ht = new Hashtable<>();
//            for(int j=0 ; j<colNames.size() ; j++){
//                ht.put(colNames.get(j), lastTuple.get(j));
//            }
//            page.insertToSorted(colNames, ht, true);
//            Serializer.serializePage(page, this.getTableName(), fileCount);
//
//        }

    }

    public void updateTuple(String strClusteringKey, Hashtable<String, Object> htblColNameValue)
            throws DBAppException, IOException, ClassNotFoundException {

        //Don't forget to check between min & max
        if(!isValidTuple(htblColNameValue))
            throw new DBAppException("The values inserted do not respect the constraints");

        if(htblColNameValue.containsKey(this.getClusteringKey()))
            throw new DBAppException("Unauthorized attempted to update clustering key");

        String directoryPath = "Serialized Database/" + this.tableName;
        File directory = new File(directoryPath);

        File folder = new File("Serialized Database/" + tableName);
        int fileCount = folder.listFiles().length;

        int index = getPageIndex(strClusteringKey);

        Page p = Serializer.deserializePage(tableName, index);
        p.updateTuple(strClusteringKey, colNames, htblColNameValue);
        Serializer.serializePage(p, this.getTableName(), index);

    }


}