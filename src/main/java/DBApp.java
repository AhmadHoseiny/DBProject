import java.io.*;
import java.util.*;

import exceptions.*;
import helper_classes.*;
import tables.*;

public class DBApp {

    public DBApp() {
        this.init();
    }

    static final String directoryPathResourcesData = "src/main/resources/Data/";

    public void init() {
        File directory = new File(directoryPathResourcesData);
        if (!directory.isDirectory())
            new File(directoryPathResourcesData).mkdirs();
    }

    public static void main(String[] args) throws DBAppException {


    }

    public void createTable(String strTableName,
                            String strClusteringKeyColumn,
                            Hashtable<String, String> htblColNameType,
                            Hashtable<String, String> htblColNameMin,
                            Hashtable<String, String> htblColNameMax)
            throws DBAppException {

        try {
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
            Table t = Serializer.deserializeTable(strTableName);
            t.deleteTuple(htblColNameValue);
            Serializer.serializeTable(t, strTableName);
        } catch (Exception e) {
            throw new DBAppException(e.getMessage());
        }
    }

    public void printTable(String strTableName) throws DBAppException {

        try {
            File folder = new File(directoryPathResourcesData + strTableName);
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

}
