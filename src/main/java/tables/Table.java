package tables;

import exceptions.DBAppException;
import helper_classes.CSVFileManipulator;
import helper_classes.ReadConfigFile;
import helper_classes.Serializer;
import index.Octree;

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
//            System.out.println(e.getKey().toLowerCase());
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

                case "java.lang.Integer":
                    isValid &= htblColNameValue.get(colNames.get(i)) instanceof Integer;
                    break;
                case "java.lang.String":
                    isValid &= htblColNameValue.get(colNames.get(i)) instanceof String;
                    break;
                case "java.lang.Double":
                case "java.lang.double":
                    isValid &= htblColNameValue.get(colNames.get(i)) instanceof Double;
                    break;
                case "java.util.Date":
                    isValid &= htblColNameValue.get(colNames.get(i)) instanceof Date;
                    break;
                default:
                    isValid &= false;

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

    public int getPageIndex(Comparable clusteringKeyVal) {
        File folder = new File(directoryPathResourcesData + tableName + "/Pages");
        int fileCount = folder.listFiles().length;

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

    public int getPageIndexHelper(Comparable clusteringKeyVal) {
        File folder = new File(directoryPathResourcesData + tableName + "/Pages");
        int fileCount = folder.listFiles().length;

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
        return hi == -1 ? 0 : hi;
    }

    public boolean checkNoNulls(Hashtable<String, Object> htblColNameValue) throws DBAppException {
        for (int i = 0; i < colNames.size(); i++) {
            if (!htblColNameValue.containsKey(colNames.get(i)))
                return false;
        }
        return true;
    }

    public void insertTuple(Hashtable<String, Object> htblColNameValue)
            throws DBAppException, IOException, ClassNotFoundException, ParseException {

        Hashtable<String, Object> newHtblCol = new Hashtable<>();
        for (Map.Entry<String, Object> e : htblColNameValue.entrySet()) {
            if (colTypes.get(colNames.indexOf(e.getKey().toLowerCase())).equals("java.lang.String"))
                newHtblCol.put(e.getKey().toLowerCase(), ((String) e.getValue()).toLowerCase());
            else
                newHtblCol.put(e.getKey().toLowerCase(), e.getValue());
        }
        htblColNameValue = newHtblCol;

        if (htblColNameValue.get(colNames.get(0)) == null)
            throw new DBAppException("Tuple has no clustering key value");

        if (!checkNoNulls(htblColNameValue)) {
            return;
        }

        //Don't forget to check between min & max
        if (!isValidTuple(htblColNameValue)) {
            throw new DBAppException("The values inserted do not respect the constraints");
        }

        String directoryPath = directoryPathResourcesData + this.tableName + "/Pages";
        File directory = new File(directoryPath);
        int fileCount = directory.listFiles().length;

        for (Map.Entry<String, Object> e : htblColNameValue.entrySet()) {
            if (!this.getColNames().contains(e.getKey()))
                throw new DBAppException("Invalid column name " + e.getKey());
        }

        HashSet<Octree> deserializedOctrees = new HashSet<>();
        HashSet<String> alreadyGotten = new HashSet<>();
        for (String index : indexNames) {

            if (index == null) {
                continue;
            }

            if (alreadyGotten.contains(index)) {
                continue;
            }

            alreadyGotten.add(index);

            Octree octree = Serializer.deserializeIndex(this, index);
            deserializedOctrees.add(octree);

        }

//       if the table is empty
        if (fileCount == 0) {

            Page page = new Page();
            this.table.add(page);
            page.insertTuple(colNames, htblColNameValue);
            minPerPage.put(0, (Comparable) htblColNameValue.get(clusteringKey));
            Serializer.serializePage(page, this.getTableName(), 0);
            insertInOctree(deserializedOctrees, page.getPage().get(0), 0, 0);
            return;

        }

        int maxPageSize = ReadConfigFile.getMaximumRowsCountInTablePage();
        int pageIndex = getPageIndex((Comparable) htblColNameValue.get(this.getClusteringKey()));
        if (pageIndex == -1) {
            pageIndex = 0;
        }

        Page curPage = Serializer.deserializePage(tableName, pageIndex);
        Vector<Object> lastTuple = curPage.insertToSorted(colNames, htblColNameValue);
        minPerPage.put(pageIndex, (Comparable) curPage.getPage().get(0).get(0));
        Serializer.serializePage(curPage, this.getTableName(), pageIndex);
        int tupleIndex;
        boolean newInsertFlag = false;
        if (lastTuple == null || (lastTuple != null && lastTuple.get(0) != htblColNameValue.get(colNames.get(0)))) {
            tupleIndex = curPage.getTupleIndex((Comparable) htblColNameValue.get(colNames.get(0)));
            insertInOctree(deserializedOctrees, curPage.getPage().get(tupleIndex), pageIndex, tupleIndex);
//            System.out.println("Inserted in page " + pageIndex + " at index " + tupleIndex);
            for (int i = tupleIndex + 1; i < Math.min(curPage.getPage().size(), maxPageSize); i++) {
                updatePointerInOctree(deserializedOctrees, curPage.getPage().get(i), pageIndex, i - 1, pageIndex, i);
            }
        }
        if (lastTuple != null && lastTuple.get(0) == htblColNameValue.get(colNames.get(0)) && pageIndex + 1 < fileCount) {
            insertInOctree(deserializedOctrees, lastTuple, pageIndex + 1, 0);
            newInsertFlag = true;
        }
//        int tupleIndex = curPage.getTupleIndex((Comparable) insertedTuple.get(0));
//        for (String index: indexNames) {
//            Vector<Comparable> keyData = new Vector<>();
//            String[] columnNames = index.split("(?=\\p{Upper})");
//            String colName1 = this.getColNamesFirstLetterCaps().get(columnNames[0]);
//            String colName2 = this.getColNamesFirstLetterCaps().get(columnNames[1]);
//            String colName3 = this.getColNamesFirstLetterCaps().get(columnNames[2]);
//
//            keyData.add((Comparable) htblColNameValue.get(colName1));
//            keyData.add((Comparable) htblColNameValue.get(colName2));
//            keyData.add((Comparable) htblColNameValue.get(colName3));
//            Octree oct = Serializer.deserializeIndex(this, index);
//            for ( int i = pageIndex; i < this.minPerPage.size(); i++) {
//                for (int j = tupleIndex; j < curPage.getPage().size(); j++) {
//
//                }
//            }
//        }
        pageIndex++;
//        boolean once = false;
        while (lastTuple != null && pageIndex < fileCount) {
            curPage = Serializer.deserializePage(tableName, pageIndex);

            lastTuple = curPage.insertAtBeginning(lastTuple);

            minPerPage.put(pageIndex, (Comparable) curPage.getPage().get(0).get(0));
            Serializer.serializePage(curPage, this.getTableName(), pageIndex);

            if (!newInsertFlag) {
                updatePointerInOctree(deserializedOctrees, curPage.getPage().get(0), pageIndex - 1, maxPageSize - 1, pageIndex, 0);
            }
            else {
                newInsertFlag = false;
            }

            for (int i = 1; i < Math.min(curPage.getPage().size(), maxPageSize); i++) {
                updatePointerInOctree(deserializedOctrees, curPage.getPage().get(i), pageIndex, i - 1, pageIndex, i);
            }
            pageIndex++;
        }
        if (lastTuple != null) {
            //create new page
            Page page = new Page();
            this.table.add(page);
            page.getPage().add(lastTuple);
            minPerPage.put(pageIndex, (Comparable) lastTuple.get(0));
            Serializer.serializePage(page, this.getTableName(), pageIndex);
            if (lastTuple.get(0) == htblColNameValue.get(colNames.get(0))) {
                insertInOctree(deserializedOctrees, lastTuple, pageIndex, 0);
            }
            else {
                updatePointerInOctree(deserializedOctrees, lastTuple, pageIndex - 1, maxPageSize - 1, pageIndex, 0);
            }
        }

        for (Octree octree : deserializedOctrees) {
            Serializer.serializeIndex(octree);
        }

    }

    public void insertInOctree(HashSet<Octree> deserializedOctrees, Vector<Object> curTuple, int pageIndex, int tupleIndex) throws IOException, DBAppException, ParseException, ClassNotFoundException {

        for (Octree octree : deserializedOctrees) {

            Vector<Comparable> keyData = new Vector<>();
            String colName1 = octree.getStrarrColName()[0];
            String colName2 = octree.getStrarrColName()[1];
            String colName3 = octree.getStrarrColName()[2];
//            System.out.println(colName1 + " " + colName2 + " " + colName3);

            keyData.add((Comparable) curTuple.get(colNames.indexOf(colName1)));
            keyData.add((Comparable) curTuple.get(colNames.indexOf(colName2)));
            keyData.add((Comparable) curTuple.get(colNames.indexOf(colName3)));
//            System.out.println("keyData1: " + keyData);
            octree.insert(keyData, pageIndex, tupleIndex);

        }

    }

    public void updatePointerInOctree(HashSet<Octree> deserializedOctrees, Vector<Object> curTuple, int oldPageIndex, int oldTupleIndex, int newPageIndex, int newTupleIndex) throws DBAppException, IOException, ParseException, ClassNotFoundException {

        for (Octree octree : deserializedOctrees) {

            Vector<Comparable> keyData = new Vector<>();
            String colName1 = octree.getStrarrColName()[0];
            String colName2 = octree.getStrarrColName()[1];
            String colName3 = octree.getStrarrColName()[2];
//            System.out.println(colName1 + " " + colName2 + " " + colName3);

            keyData.add((Comparable) curTuple.get(colNames.indexOf(colName1)));
            keyData.add((Comparable) curTuple.get(colNames.indexOf(colName2)));
            keyData.add((Comparable) curTuple.get(colNames.indexOf(colName3)));
//            System.out.println("keyData1: " + keyData);
            octree.updatePointer(keyData, oldPageIndex, oldTupleIndex, newPageIndex, newTupleIndex);

        }
    }

    public void updateTuple(String strClusteringKey, Hashtable<String, Object> htblColNameValue)
            throws DBAppException, IOException, ClassNotFoundException, ParseException {

        Hashtable<String, Object> newHtblCol = new Hashtable<>();
        for (Map.Entry<String, Object> e : htblColNameValue.entrySet()) {
            if (colTypes.get(colNames.indexOf(e.getKey().toLowerCase())).equals("java.lang.String"))
                newHtblCol.put(e.getKey().toLowerCase(), ((String) e.getValue()).toLowerCase());
            else
                newHtblCol.put(e.getKey().toLowerCase(), e.getValue());
        }
        htblColNameValue = newHtblCol;

        //Don't forget to check between min & max
        if (!isValidTuple(htblColNameValue))
            throw new DBAppException("The values inserted do not respect the constraints");

        if (strClusteringKey == null)
            throw new DBAppException("Tuple has no clustering key value");

        if (htblColNameValue.containsKey(this.getClusteringKey()))
            throw new DBAppException("Unauthorized attempted to update clustering key");

        for (Map.Entry<String, Object> e : htblColNameValue.entrySet()) {
            if (!this.getColNames().contains(e.getKey()))
                throw new DBAppException("Invalid column name " + e.getKey());
        }

        Comparable clusteringKeyVal;
        switch (colTypes.get(0)) {

            case "java.lang.Integer":
                clusteringKeyVal = Integer.parseInt(strClusteringKey);
                break;
            case "java.lang.String":
                clusteringKeyVal = strClusteringKey;
                break;
            case "java.lang.Double":
            case "java.lang.double":
                clusteringKeyVal = Double.parseDouble(strClusteringKey);
                break;
            case "java.util.Date":
                String pattern = "yyyy-MM-dd";
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
                clusteringKeyVal = simpleDateFormat.parse(strClusteringKey);
                break;
            default:
                throw new DBAppException("Invalid clustering key type");

        }
        int index = getPageIndex(clusteringKeyVal);
        if (index == -1) //clustering key does not exist
            return;

        Page p = Serializer.deserializePage(tableName, index);
        p.updateTuple(clusteringKeyVal, colNames, htblColNameValue);
        Serializer.serializePage(p, this.getTableName(), index);

    }

    //implement a method that takes a clustering key and deletes its tuple from a specific page after deserializnig it and then serializing it again after deleting the tuple from it
    public void deleteTuple(Hashtable<String, Object> htblColNameValue) throws DBAppException, IOException, ClassNotFoundException {

        Hashtable<String, Object> newHtblCol = new Hashtable<>();
        for (Map.Entry<String, Object> e : htblColNameValue.entrySet()) {
            if (colTypes.get(colNames.indexOf(e.getKey().toLowerCase())).equals("java.lang.String"))
                newHtblCol.put(e.getKey().toLowerCase(), ((String) e.getValue()).toLowerCase());
            else
                newHtblCol.put(e.getKey().toLowerCase(), e.getValue());
        }
        htblColNameValue = newHtblCol;

        String directoryPath = directoryPathResourcesData + this.tableName;
        File directory = new File(directoryPath);

        if (!directory.isDirectory()) {
            throw new DBAppException("This table does not exist!");
        }
        if (!isValidTupleType(htblColNameValue)) { // The values you are trying to delete do not respect the constraints
            throw new DBAppException("The values you are trying to delete do not respect the constraints");
        }
        File folder = new File(directoryPathResourcesData + tableName + "/Pages");
        int fileCount = folder.listFiles().length;
        //if the table is empty, we delete all records (truncate)
        if (htblColNameValue.isEmpty()) {
            for (int i = 0; i < fileCount; i++) {
                File fileToDelete = new File(directoryPathResourcesData + tableName + "/Pages/Page_" + i + ".ser");
                fileToDelete.delete();
            }
            minPerPage.clear();
            return;
        }

        for (Map.Entry<String, Object> e : htblColNameValue.entrySet()) {
            if (!this.getColNames().contains(e.getKey()))
                throw new DBAppException("Invalid column name " + e.getKey());
        }

        //a unique row
        if (htblColNameValue.containsKey(this.getClusteringKey())) {
            int index = getPageIndex((Comparable) htblColNameValue.get(getColNames().get(0)));
            if (index == -1) { // No matching Tuples to be deleted exist
                return;
            }
            Page p = Serializer.deserializePage(tableName, index);
            boolean nonEmptyPage = p.deleteSingleTuple((Comparable) htblColNameValue.get(getColNames().get(0)), colNames, htblColNameValue);
            if (nonEmptyPage) {
                Comparable minKey = (Comparable) p.getPage().get(0).get(0);
                minPerPage.put(index, minKey);
                Serializer.serializePage(p, this.getTableName(), index);
            } else {
                File fileToDelete = new File(directoryPathResourcesData + tableName + "/Pages/Page_" + index + ".ser");
                fileToDelete.delete();
                for (int i = index + 1; i < fileCount; i++) {
                    File fileToRename = new File(directoryPathResourcesData + tableName + "/Pages/Page_" + i + ".ser");
                    File newFile = new File(directoryPathResourcesData + tableName + "/Pages/Page_" + (i - 1) + ".ser");
                    fileToRename.renameTo(newFile);
                    minPerPage.put(i - 1, minPerPage.get(i));
                }
                minPerPage.remove(fileCount - 1);

            }
        } else {
            LinkedList<Integer> pagesToDelete = new LinkedList<>();
            for (int i = 0; i < fileCount; i++) {
                Page p = Serializer.deserializePage(tableName, i);
                boolean nonEmptyPage = p.deleteAllMatchingTuples(colNames, htblColNameValue);
                if (nonEmptyPage) {
                    Comparable minKey = (Comparable) p.getPage().get(0).get(0);
                    minPerPage.put(i, minKey);
                    Serializer.serializePage(p, this.getTableName(), i);
                } else {
                    pagesToDelete.add(i);
                }
            }

            int j = 0;
            for (int i = 0; i < fileCount; i++) {
                if (!pagesToDelete.isEmpty() && i == pagesToDelete.peekFirst()) {
                    File fileToDelete = new File(directoryPathResourcesData + tableName + "/Pages/Page_" + i + ".ser");
                    fileToDelete.delete();
                    pagesToDelete.removeFirst();
                    j++;
                } else {
                    File fileToRename = new File(directoryPathResourcesData + tableName + "/Pages/Page_" + i + ".ser");
                    File newFile = new File(directoryPathResourcesData + tableName + "/Pages/Page_" + (i - j) + ".ser");
                    fileToRename.renameTo(newFile);
                    minPerPage.put(i - j, minPerPage.get(i));
                }
            }
            for (int i = 0; i < j; i++) {
                minPerPage.remove(fileCount - 1 - i);
            }

        }

    }

}