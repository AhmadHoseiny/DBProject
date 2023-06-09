package table;

import exceptions.DBAppException;
import helper_classes.CSVFileManipulator;
import index.Octree;
import page.Page;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Table implements Serializable {
    static final String directoryPathResourcesData = "src/main/resources/Data/";
    private final String tableName;
    private final String clusteringKey;
    private Vector<String> colNames;
    private transient HashMap<String, String> colNamesFirstLetterCaps;
    private transient Vector<String> colTypes;
    private transient Vector<Comparable> colMin;
    private transient Vector<Comparable> colMax;
    private transient Vector<String> indexNames; // null represents no index
    private HashMap<Integer, Comparable> minPerPage;
    private transient Vector<Page> table;

    public Table(String strTableName,
                 String strClusteringKeyColumn,
                 Hashtable<String, String> htblColNameType,
                 Hashtable<String, String> htblColNameMin,
                 Hashtable<String, String> htblColNameMax) throws DBAppException, IOException, ParseException {

        if (htblColNameType.size() != htblColNameMin.size() ||
                htblColNameMin.size() != htblColNameMax.size() ||
                htblColNameType.size() != htblColNameMax.size())
            throw new DBAppException("The input data is inconsistent");

        // Initialize table
        this.tableName = strTableName;
        this.clusteringKey = strClusteringKeyColumn;
        this.colNames = new Vector<>();
        this.minPerPage = new HashMap<>();
        colNames.add(this.clusteringKey.toLowerCase());
        for (Map.Entry<String, String> e : htblColNameType.entrySet()) {
            if (e.getKey().equals(this.clusteringKey))
                continue;
            colNames.add(e.getKey().toLowerCase());
        }

        Hashtable<String, String> newHtblColNameMin = newHashtableCreator(htblColNameType, htblColNameMin);
        Hashtable<String, String> newHtblColNameMax = newHashtableCreator(htblColNameType, htblColNameMax);
        Hashtable<String, String> newHtblColNameType = new Hashtable<>();

        for (Map.Entry<String, String> e : htblColNameType.entrySet()) {
            newHtblColNameType.put(e.getKey().toLowerCase(), e.getValue());
        }

        CSVFileManipulator.write(strTableName, newHtblColNameType, newHtblColNameMin, newHtblColNameMax, colNames);
        this.initializeTable();
    }

    public Hashtable<String, String> newHashtableCreator(Hashtable<String, String> htblColNameType,
                                                         Hashtable<String, String> htblColNameMnMx) {
        Hashtable<String, String> newHtblColNameMnMx = new Hashtable<>();
        for (Map.Entry<String, String> e : htblColNameMnMx.entrySet()) {
            if (htblColNameType.get(e.getKey()).equals("java.lang.String")) {
                newHtblColNameMnMx.put(e.getKey().toLowerCase(), e.getValue().toLowerCase());
            } else {
                newHtblColNameMnMx.put(e.getKey().toLowerCase(), e.getValue());
            }
        }
        return newHtblColNameMnMx;
    }

    public HashMap<String, String> getColNamesFirstLetterCaps() {
        return colNamesFirstLetterCaps;
    }

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

    public Vector<String> getIndexNames() {
        return indexNames;
    }

    public HashMap<Integer, Comparable> getMinPerPage() {
        return minPerPage;
    }

    public void initializeTable() throws IOException, ParseException {
        this.colTypes = new Vector<>();
        this.colMin = new Vector<>();
        this.colMax = new Vector<>();
        this.indexNames = new Vector<>();
        this.colNamesFirstLetterCaps = new HashMap<>();
        for (String colName : colNames) {
            String colNameFirstLetterCap = colName.substring(0, 1).toUpperCase() + colName.substring(1).toLowerCase();
            this.colNamesFirstLetterCaps.put(colNameFirstLetterCap, colName);
        }
        CSVFileManipulator.read(this.tableName, this.colNames,
                this.colTypes, this.colMin, this.colMax,
                this.indexNames);
        this.table = new Vector<>();
    }

    public boolean isValidTupleType(Hashtable<String, Object> htblColNameValue) {
        boolean isValid = true;
        for (int i = 0; i < colNames.size(); i++) {
            if (!htblColNameValue.containsKey(colNames.get(i)))
                continue;

            switch (colTypes.get(i)) {
                case "java.lang.Integer" -> isValid &= htblColNameValue.get(colNames.get(i)) instanceof Integer;
                case "java.lang.String" -> isValid &= htblColNameValue.get(colNames.get(i)) instanceof String;
                case "java.lang.Double", "java.lang.double" ->
                        isValid &= htblColNameValue.get(colNames.get(i)) instanceof Double;
                case "java.util.Date" -> isValid &= htblColNameValue.get(colNames.get(i)) instanceof Date;
                default -> isValid = false;
            }
        }
        return isValid;
    }

    public boolean isValidTupleMinMax(Hashtable<String, Object> htblColNameValue) {
        boolean isValid = true;

        for (int i = 0; i < colNames.size(); i++) {

            if (!htblColNameValue.containsKey(colNames.get(i)))
                continue;
            isValid &= ((Comparable) htblColNameValue.get(colNames.get(i))).compareTo(((Comparable) colMin.get(i))) >= 0;
            isValid &= ((Comparable) htblColNameValue.get(colNames.get(i))).compareTo(((Comparable) colMax.get(i))) <= 0;

        }

        return isValid;
    }

    public boolean isValidTuple(Hashtable<String, Object> htblColNameValue) {

        return isValidTupleType(htblColNameValue) && isValidTupleMinMax(htblColNameValue);

    }

    public void validate(Hashtable<String, Object> htblColNameValue) throws DBAppException {

        for (Map.Entry<String, Object> e : htblColNameValue.entrySet()) {
            if (!this.getColNames().contains(e.getKey().toLowerCase()))
                throw new DBAppException("Invalid column name " + e.getKey());
        }

        for (Map.Entry<String, Object> e : htblColNameValue.entrySet()) {
            if (colTypes.get(colNames.indexOf(e.getKey().toLowerCase())).equals("java.lang.String"))
                htblColNameValue.put(e.getKey().toLowerCase(), ((String) e.getValue()).toLowerCase());
            else
                htblColNameValue.put(e.getKey().toLowerCase(), e.getValue());
        }

    }

    public int getPageIndex(Comparable clusteringKeyVal) {
        File folder = new File(directoryPathResourcesData + tableName + "/Pages");
        int fileCount = Objects.requireNonNull(folder.listFiles()).length;

        int lo = 0;
        int hi = fileCount - 1;

        while (lo <= hi) {
            int mid = lo + (hi - lo >> 1);
            if (minPerPage.get(mid).compareTo(clusteringKeyVal) > 0) {
                hi = mid - 1;
            } else {
                lo = mid + 1;
            }
        }

        return hi;
    }

    public boolean checkNoNulls(Hashtable<String, Object> htblColNameValue) throws DBAppException {
        for (String colName : colNames) {
            if (!htblColNameValue.containsKey(colName))
                return false;
        }
        return true;
    }

    public void insertTuple(Hashtable<String, Object> htblColNameValue)
            throws DBAppException, IOException, ClassNotFoundException, ParseException {

//        Hashtable<String, Object> newHtblCol = new Hashtable<>();
//        for (Map.Entry<String, Object> e : htblColNameValue.entrySet()) {
//            if (colTypes.get(colNames.indexOf(e.getKey().toLowerCase())).equals("java.lang.String"))
//                newHtblCol.put(e.getKey().toLowerCase(), ((String) e.getValue()).toLowerCase());
//            else
//                newHtblCol.put(e.getKey().toLowerCase(), e.getValue());
//        }
//        htblColNameValue = newHtblCol;

        validate(htblColNameValue);

        if (!htblColNameValue.containsKey(colNames.get(0)))
            throw new DBAppException("Tuple has no clustering key value");

        //nulls can be inserted
//        if (!checkNoNulls(htblColNameValue)) {
//            return;
//        }

        //Don't forget to check between min & max
        if (!isValidTuple(htblColNameValue)) {
            throw new DBAppException("The values inserted do not respect the constraints");
        }

        TableInserter inserter = new TableInserter(this);
        inserter.insertTuple(htblColNameValue);

    }

    public void insertInOctree(HashSet<Octree> deserializedOctrees, Vector<Object> curTuple, int pageIndex, int tupleIndex) throws IOException, DBAppException, ParseException, ClassNotFoundException {

        for (Octree octree : deserializedOctrees) {

            Vector<Comparable> keyData = new Vector<>();
            String colName1 = octree.getStrarrColName()[0];
            String colName2 = octree.getStrarrColName()[1];
            String colName3 = octree.getStrarrColName()[2];

            keyData.add((Comparable) curTuple.get(colNames.indexOf(colName1)));
            keyData.add((Comparable) curTuple.get(colNames.indexOf(colName2)));
            keyData.add((Comparable) curTuple.get(colNames.indexOf(colName3)));
            octree.insert(keyData, pageIndex, tupleIndex);

        }

    }

    public void deleteInOctree(HashSet<Octree> deserializedOctrees, Vector<Object> curTuple, int pageIndex, int tupleIndex) throws IOException, DBAppException, ParseException, ClassNotFoundException {

        for (Octree octree : deserializedOctrees) {
            Vector<Comparable> keyData = new Vector<>();
            String colName1 = octree.getStrarrColName()[0];
            String colName2 = octree.getStrarrColName()[1];
            String colName3 = octree.getStrarrColName()[2];

            keyData.add((Comparable) curTuple.get(colNames.indexOf(colName1)));
            keyData.add((Comparable) curTuple.get(colNames.indexOf(colName2)));
            keyData.add((Comparable) curTuple.get(colNames.indexOf(colName3)));
            octree.delete(keyData, pageIndex, tupleIndex);

        }

    }

    public void updatePointerInOctree(HashSet<Octree> deserializedOctrees, Vector<Object> curTuple, int oldPageIndex, int oldTupleIndex, int newPageIndex, int newTupleIndex) throws DBAppException, IOException, ParseException, ClassNotFoundException {

        for (Octree octree : deserializedOctrees) {
            Vector<Comparable> keyData = new Vector<>();
            String colName1 = octree.getStrarrColName()[0];
            String colName2 = octree.getStrarrColName()[1];
            String colName3 = octree.getStrarrColName()[2];

            keyData.add((Comparable) curTuple.get(colNames.indexOf(colName1)));
            keyData.add((Comparable) curTuple.get(colNames.indexOf(colName2)));
            keyData.add((Comparable) curTuple.get(colNames.indexOf(colName3)));
            octree.updatePointer(keyData, oldPageIndex, oldTupleIndex, newPageIndex, newTupleIndex);

        }
    }

    public void updateTuple(String strClusteringKey, Hashtable<String, Object> htblColNameValue)
            throws DBAppException, IOException, ClassNotFoundException, ParseException {

//        Hashtable<String, Object> newHtblCol = new Hashtable<>();
//        for (Map.Entry<String, Object> e : htblColNameValue.entrySet()) {
//            if (colTypes.get(colNames.indexOf(e.getKey().toLowerCase())).equals("java.lang.String"))
//                newHtblCol.put(e.getKey().toLowerCase(), ((String) e.getValue()).toLowerCase());
//            else
//                newHtblCol.put(e.getKey().toLowerCase(), e.getValue());
//        }
//        htblColNameValue = newHtblCol;

        validate(htblColNameValue);

        if (strClusteringKey == null)
            throw new DBAppException("Tuple has no clustering key value");

        if (htblColNameValue.containsKey(this.getClusteringKey()))
            throw new DBAppException("Unauthorized attempted to update clustering key");

        //Don't forget to check between min & max
        if (!isValidTuple(htblColNameValue)) {
            throw new DBAppException("The values inserted do not respect the constraints");
        }

        Comparable clusteringKeyVal;
        switch (colTypes.get(0)) {
            case "java.lang.Integer" -> clusteringKeyVal = Integer.parseInt(strClusteringKey);
            case "java.lang.String" -> clusteringKeyVal = strClusteringKey;
            case "java.lang.Double", "java.lang.double" -> clusteringKeyVal = Double.parseDouble(strClusteringKey);
            case "java.util.Date" -> {
                String pattern = "yyyy-MM-dd";
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
                clusteringKeyVal = simpleDateFormat.parse(strClusteringKey);
            }
            default -> throw new DBAppException("Invalid clustering key type");
        }

        TableUpdater updater = new TableUpdater(this);
        updater.updateTuple(clusteringKeyVal, htblColNameValue);


    }

    //implement a method that takes a clustering key and deletes its tuple from a specific page after deserializnig it and then serializing it again after deleting the tuple from it
    public void deleteTuple(Hashtable<String, Object> htblColNameValue) throws DBAppException, IOException, ClassNotFoundException, ParseException {

        validate(htblColNameValue);

        String directoryPath = directoryPathResourcesData + this.tableName;
        File directory = new File(directoryPath);

        if (!directory.isDirectory()) {
            throw new DBAppException("This table does not exist!");
        }
        if (!isValidTupleType(htblColNameValue)) { // The values you are trying to delete do not respect the constraints
            throw new DBAppException("The values you are trying to delete do not respect the constraints");
        }

        TableDeleter deleter = new TableDeleter(this);
        deleter.deleteTuple(htblColNameValue);

    }

    public Octree getIndexToBeUsed(Hashtable<String, Object> htblColNameValue,
                                   HashSet<Octree> deserializedOctrees, boolean colsToBeTaken[]) {
        for (int i = 3; i >= 1; i--) {
            for (Octree octree : deserializedOctrees) {
                int cnt = 0;
                Arrays.fill(colsToBeTaken, false);
                for (int j = 0; j < 3; j++) {
                    String colName = octree.getStrarrColName()[j];
                    if (htblColNameValue.containsKey(colName)) {
                        cnt++;
                        colsToBeTaken[j] = true;
                    }
                }
                if (cnt == i) {
                    return octree;
                }
            }
        }
        return null; //no applicable octrees found
    }

}
