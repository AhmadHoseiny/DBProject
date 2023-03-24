package tables;

import java.io.*;
import java.util.*;

import exceptions.DBAppException;
import helper_classes.*;

public class Table implements Serializable {
    private final String tableName;
    private final String clusteringKey;
    private Vector<String> colNames;
    private Vector<String> colTypes;
    private Vector<Comparable> colMin;
    private Vector<Comparable> colMax;
    private HashMap<Integer, Comparable> minPerPage;
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

    public Vector<Comparable> getColMin() {
        return colMin;
    }

    public Vector<Comparable> getColMax() {
        return colMax;
    }

    public Vector<Page> getTable() {
        return table;
    }

    public void initializeTable() {
        this.table = new Vector<>();
    }

    public HashMap<Integer, Comparable> getMinPerPage() {
        return minPerPage;
    }

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
        this.minPerPage = new HashMap<>();

        colNames.add(this.clusteringKey);
        colTypes.add(htblColNameType.get(this.clusteringKey));

        for(Map.Entry<String, String> e : htblColNameType.entrySet()){

            if(e.getKey().equals(this.clusteringKey)) {
                continue;
            }
            colNames.add(e.getKey());
            colTypes.add(e.getValue());

        }



        for(int i = 0; i < colNames.size(); i++){

            Comparable colMinVal;
            Comparable colMaxVal;

            switch (colTypes.get(i)) {

                case "java.lang.Integer":
                    colMinVal = Integer.parseInt(htblColNameMin.get(colNames.get(i)));
                    colMaxVal = Integer.parseInt(htblColNameMax.get(colNames.get(i)));
                    break;
                case "java.lang.String":
                    colMinVal = htblColNameMin.get(colNames.get(i));
                    colMaxVal = htblColNameMax.get(colNames.get(i));
                    break;
                case "java.lang.Double":
                case "java.lang.double":
                    colMinVal = Double.parseDouble(htblColNameMin.get(colNames.get(i)));
                    colMaxVal = Double.parseDouble(htblColNameMax.get(colNames.get(i)));
                    break;
                case "java.util.Date":
                    colMinVal = Date.parse(htblColNameMin.get(colNames.get(i)));
                    colMaxVal = Date.parse(htblColNameMax.get(colNames.get(i)));
                    break;
                default:
                    throw new DBAppException("Invalid column type");
            }

            colMin.add(colMinVal);
            colMax.add(colMaxVal);

        }
        
        this.table = new Vector<>();
        
    }


    public boolean isValidTuple(Hashtable<String, Object> htblColNameValue) {

        boolean isValid = true;

        for(int i = 0; i < colNames.size(); i++){

            if (!htblColNameValue.containsKey(colNames.get(i)))
                continue;

            switch (colTypes.get(i)) {

                case "java.lang.Integer" : isValid &= htblColNameValue.get(colNames.get(i)) instanceof Integer; break;
                case "java.lang.String" : isValid &= htblColNameValue.get(colNames.get(i)) instanceof String; break;
                case "java.lang.Double" :
                case "java.lang.double" : isValid &= htblColNameValue.get(colNames.get(i)) instanceof Double; break;
                case "java.util.Date" : isValid &= htblColNameValue.get(colNames.get(i)) instanceof Date; break;
                default: isValid &= false;

            }
            
            isValid &= ((Comparable)htblColNameValue.get(colNames.get(i))).compareTo(((Comparable)colMin.get(i)))>=0 ;
            isValid &= ((Comparable)htblColNameValue.get(colNames.get(i))).compareTo(((Comparable)colMax.get(i)))<=0 ;

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
            if (minPerPage.get(mid).compareTo(clusteringKeyVal) > 0) {
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
        int fileCount = directory.listFiles().length;

//       if the table is empty
        if(fileCount == 0){

            Page page = new Page();
            this.table.add(page);
            page.insertTuple(colNames, htblColNameValue);
            minPerPage.put(0, (Comparable) htblColNameValue.get(clusteringKey));
            Serializer.serializePage(page, this.getTableName(), 0);
            return;

        }

        int pageIndex = getPageIndex((Comparable) htblColNameValue.get(this.getClusteringKey()));
        if (pageIndex == -1) {
            pageIndex = 0;
        }

        Page curPage = Serializer.deserializePage(tableName, pageIndex);
        Vector<Object> lastTuple  = curPage.insertToSorted(colNames, htblColNameValue);
        minPerPage.put(pageIndex, (Comparable) curPage.getPage().get(0).get(0));
        Serializer.serializePage(curPage, this.getTableName(), pageIndex);
        pageIndex++;
        while(lastTuple != null && pageIndex<fileCount){
            curPage = Serializer.deserializePage(tableName, pageIndex);
            lastTuple  = curPage.insertAtBeginning(lastTuple);
            minPerPage.put(pageIndex, (Comparable) curPage.getPage().get(0).get(0));
            Serializer.serializePage(curPage, this.getTableName(), pageIndex);
            pageIndex++;
        }
        if(lastTuple != null){
            //create new page
            Page page = new Page();
            this.table.add(page);
            page.getPage().add(lastTuple);
            minPerPage.put(pageIndex, (Comparable) lastTuple.get(0));
            Serializer.serializePage(page, this.getTableName(), pageIndex);
        }

    }

    public void updateTuple(String strClusteringKey, Hashtable<String, Object> htblColNameValue)
            throws DBAppException, IOException, ClassNotFoundException {

        //Don't forget to check between min & max
        if(!isValidTuple(htblColNameValue))
            throw new DBAppException("The values inserted do not respect the constraints");

        if(htblColNameValue.containsKey(this.getClusteringKey()))
            throw new DBAppException("Unauthorized attempted to update clustering key");

       Comparable clusteringKeyVal;
        switch (colTypes.get(0)) {

            case "java.lang.Integer" : clusteringKeyVal = Integer.parseInt(strClusteringKey); break;
            case "java.lang.String" : clusteringKeyVal = strClusteringKey; break;
            case "java.lang.Double" :
            case "java.lang.double" : clusteringKeyVal = Double.parseDouble(strClusteringKey); break;
            case "java.util.Date" : clusteringKeyVal = Date.parse(strClusteringKey); break;
            default: throw new DBAppException("Invalid clustering key type");

        }
        int index = getPageIndex(clusteringKeyVal);


        Page p = Serializer.deserializePage(tableName, index);
        p.updateTuple(clusteringKeyVal, colNames, htblColNameValue);
        Serializer.serializePage(p, this.getTableName(), index);

    }

    //implement a method that takes a clustering key and deletes its tuple from a specific page after deserializnig it and then serializing it again after deleting the tuple from it
    public void deleteTuple(Hashtable<String, Object> htblColNameValue) throws DBAppException, IOException, ClassNotFoundException {

        String directoryPath = "Serialized Database/" + this.tableName;
        File directory = new File(directoryPath);

        if(!directory.isDirectory()) {
            throw new DBAppException("This table does not exist!");
        }


        //a unique row
        if (htblColNameValue.containsKey(getClusteringKey())){
            int index = getPageIndex((Comparable) htblColNameValue.get(getColNames().get(0)));
            if (index == -1) {
                throw new DBAppException("Tuple does not exist");
            }
            Page p = Serializer.deserializePage(tableName, index);
            boolean nonEmptyPage = p.deleteSingleTuple((Comparable) htblColNameValue.get(getColNames().get(0)), colNames, htblColNameValue);
            if (nonEmptyPage) {
                Comparable minKey = (Comparable) p.getPage().get(0).get(0);
                minPerPage.put(index, minKey);
                Serializer.serializePage(p, this.getTableName(), index);
            }
            else {
                File folder = new File("Serialized Database/" + tableName);
                int fileCount = folder.listFiles().length;
                File fileToDelete = new File("Serialized Database/" + tableName + "/Page_" + index + ".ser");
                fileToDelete.delete();
                for(int i=index + 1 ; i<fileCount ; i++){
                    File fileToRename = new File("Serialized Database/" + tableName + "/Page_" + i + ".ser");
                    File newFile = new File("Serialized Database/" + tableName + "/Page_" + (i-1) + ".ser");
                    fileToRename.renameTo(newFile);
                    minPerPage.put(i-1, minPerPage.get(i));
                }
                minPerPage.remove(fileCount-1);
                
            }
        }
        else{
            File folder = new File("Serialized Database/" + tableName);
            int fileCount = folder.listFiles().length;
            LinkedList<Integer> pagesToDelete = new LinkedList<>();
            for(int i=0 ; i<fileCount ; i++){
                Page p = Serializer.deserializePage(tableName, i);
                boolean nonEmptyPage = p.deleteAllMatchingTuples(colNames, htblColNameValue);
                if (nonEmptyPage) {
                    Comparable minKey = (Comparable) p.getPage().get(0).get(0);
                    minPerPage.put(i, minKey);
                    Serializer.serializePage(p, this.getTableName(), i);
                }
                else {
                    pagesToDelete.add(i);
                }
            }

            int j = 0;
            for (int i = 0; i < fileCount; i++) {
                if(!pagesToDelete.isEmpty() && i==pagesToDelete.peekFirst()) {
                    File fileToDelete = new File("Serialized Database/" + tableName + "/Page_" + i + ".ser");
                    fileToDelete.delete();
                    pagesToDelete.removeFirst();
                    j++;
                }
                else {
                    File fileToRename = new File("Serialized Database/" + tableName + "/Page_" + i + ".ser");
                    File newFile = new File("Serialized Database/" + tableName + "/Page_" + (i-j) + ".ser");
                    fileToRename.renameTo(newFile);
                    minPerPage.put(i-j, minPerPage.get(i));
                }
            }
            for(int i=0 ; i<j ; i++){
                minPerPage.remove(fileCount-1-i);
            }

        }
        
    }
    
}