import exceptions.DBAppException;
import helper_classes.*;
import index.Octree;
import page.Page;
import parser.MySQLParser;
import table.Table;

import java.io.File;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

public class DBApp {

    static final String directoryPathResourcesData = "src/main/resources/Data/";

    public DBApp() {
        this.init();
    }

    public static void main(String[] args) throws DBAppException {

    }

    public void init() {
        File directory = new File(directoryPathResourcesData);
        if (!directory.isDirectory())
            new File(directoryPathResourcesData).mkdirs();
    }

    public void createTable(String strTableName,
                            String strClusteringKeyColumn,
                            Hashtable<String, String> htblColNameType,
                            Hashtable<String, String> htblColNameMin,
                            Hashtable<String, String> htblColNameMax)
            throws DBAppException {

        try {
            strTableName = strTableName.toLowerCase();
            strClusteringKeyColumn = strClusteringKeyColumn.toLowerCase();
            File tableToCreate = new File(directoryPathResourcesData + strTableName + ".ser");
            if (tableToCreate.exists()) {
                throw new DBAppException("Table already exists");
            }
            Table t = new Table(strTableName, strClusteringKeyColumn, htblColNameType, htblColNameMin, htblColNameMax);
            Serializer.serializeTable(t, strTableName);
        } catch (Exception e) {
            throw new DBAppException(e.getMessage());
        }

    }

    public void insertIntoTable(String strTableName,
                                Hashtable<String, Object> htblColNameValue)
            throws DBAppException {
        try {
            strTableName = strTableName.toLowerCase();
            Table t = Serializer.deserializeTable(strTableName);
            t.insertTuple(htblColNameValue);
            Serializer.serializeTable(t, strTableName);
        } catch (Exception e) {
            throw new DBAppException(e.getMessage());
        }
    }

    public void updateTable(String strTableName,
                            String strClusteringKeyValue,
                            Hashtable<String, Object> htblColNameValue)
            throws DBAppException {
        try {
            strTableName = strTableName.toLowerCase();
            Table t = Serializer.deserializeTable(strTableName);
            t.updateTuple(strClusteringKeyValue, htblColNameValue);
            Serializer.serializeTable(t, strTableName);
        } catch (Exception e) {
            throw new DBAppException(e.getMessage());
        }
    }

    public void deleteFromTable(String strTableName,
                                Hashtable<String, Object> htblColNameValue)
            throws DBAppException {
        try {
            strTableName = strTableName.toLowerCase();
            Table t = Serializer.deserializeTable(strTableName);
            t.deleteTuple(htblColNameValue);
            Serializer.serializeTable(t, strTableName);
        } catch (Exception e) {
            throw new DBAppException(e.getMessage());
        }
    }

    public void printTable(String strTableName) throws DBAppException {

        try {
            strTableName = strTableName.toLowerCase();
            File folder = new File(directoryPathResourcesData + strTableName + "/Pages");
            int fileCount = folder.listFiles().length;

            for (int i = 0; i < fileCount; i++) {
                Page p;
                p = Serializer.deserializePage(strTableName, i);
                System.out.println(p.getPage());
                Serializer.serializePage(p, strTableName, i);
            }
        } catch (Exception e) {
            throw new DBAppException(e.getMessage());
        }

    }

    public Iterator selectFromTable(SQLTerm[] arrSQLTerms,
                                    String[] strarrOperators)
            throws DBAppException {

        try {
            for (int i = 0; i < arrSQLTerms.length; i++) {
                arrSQLTerms[i]._strTableName = arrSQLTerms[i]._strTableName.toLowerCase();
                arrSQLTerms[i]._strColumnName = arrSQLTerms[i]._strColumnName.toLowerCase();
                if (arrSQLTerms[i]._objValue instanceof String) {
                    arrSQLTerms[i]._objValue = ((String) arrSQLTerms[i]._objValue).toLowerCase();
                }
            }
            for (int i = 0; i < strarrOperators.length; i++) {
                strarrOperators[i] = strarrOperators[i].toLowerCase();
            }

            MyIterator it = new MyIterator(arrSQLTerms, strarrOperators);
            return it;
        } catch (Exception e) {
            throw new DBAppException(e.getMessage());
        }

    }


    public void createIndex(String strTableName,
                            String[] strarrColName) throws DBAppException {
        try {
            strTableName = strTableName.toLowerCase();
            File tableToCreate = new File(directoryPathResourcesData + strTableName + ".ser");
            //check that the table exists
            if (!tableToCreate.exists()) {
                throw new DBAppException("Table doesn't exist");
            }
            for (int i = 0; i < strarrColName.length; i++) {
                strarrColName[i] = strarrColName[i].toLowerCase();
            }
            Arrays.sort(strarrColName);
            String indexName = IndexNameGetter.getIndexName(strarrColName);
            File indexToCreate = new File(directoryPathResourcesData +
                    strTableName + "/Indices/" + indexName + ".ser");
            //check that the index doesn't exist
            if (indexToCreate.exists()) {
                throw new DBAppException("Index already exists");
            }

            //check that the index is on 3 columns
            if (strarrColName.length != 3) {
                throw new DBAppException("Index can only be created on 3 columns");
            }

            //check that the columns exist in the table
            Table t = Serializer.deserializeTable(strTableName);
            Vector<String> tableColNames = t.getColNames();
            for (int i = 0; i < strarrColName.length; i++) {
                if (!tableColNames.contains(strarrColName[i])) {
                    throw new DBAppException("Column " + strarrColName[i] + " doesn't exist in table " + strTableName);
                }
                if (t.getIndexNames() != null && t.getIndexNames().get(t.getColNames().indexOf(strarrColName[i])) != null) {
                    throw new DBAppException("Column " + strarrColName[i] + " already has an index");
                }
            }
            Octree ot = new Octree(t, strarrColName);

            AllRecordInIndexInserter inserter = new AllRecordInIndexInserter(t, ot);
            inserter.insertAllRecords();
            Serializer.serializeIndex(ot);
            Serializer.serializeTable(t, strTableName);
        } catch (Exception e) {
            throw new DBAppException(e.getMessage());
        }

    }

    public void printIndex(String strTableName, String[] strarrColName) throws DBAppException {
        try {
            strTableName = strTableName.toLowerCase();
            Arrays.sort(strarrColName);
            String indexName = IndexNameGetter.getIndexName(strarrColName);
            Table t = Serializer.deserializeTable(strTableName);
            Octree ot = Serializer.deserializeIndex(t, indexName);
            ot.printIndexDFS();
        } catch (Exception e) {
            throw new DBAppException(e.getMessage());
        }
    }

    public Iterator parseSQL(StringBuffer strbufSQL) throws
            DBAppException {
        return MySQLParser.parse(strbufSQL);
    }

}
