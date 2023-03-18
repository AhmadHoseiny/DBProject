import java.io.*;
import java.util.*;

import exceptions.*;
import helper_classes.*;
import tables.*;

public class DBApp {

    public DBApp() {
//        this.init();
    }

    public void init() {
        String directoryPath = "Serialized Database/";
        new File(directoryPath).mkdirs();
    }

    public static void main(String[] args) throws IOException, DBAppException, ClassNotFoundException {

        Hashtable htblColNameType = new Hashtable( );
        htblColNameType.put("id", "java.lang.Integer");
        htblColNameType.put("name", "java.lang.String");
        htblColNameType.put("gpa", "java.lang.double");

        Hashtable<String, String> htblColNameMin = new Hashtable<>();
        htblColNameMin.put("id", "1");
        htblColNameMin.put("name", "A");
        htblColNameMin.put("gpa", "0.01");

        Hashtable<String, String> htblColNameMax = new Hashtable<>();
        htblColNameMax.put("id", "100");
        htblColNameMax.put("name", "zzzzzzzzzzzzzzzzzzzzz");
        htblColNameMax.put("gpa", "6.0");

        DBApp dbApp = new DBApp();
//        dbApp.createTable("Student", "id", htblColNameType,
//                htblColNameMin, htblColNameMax);
//
//        Hashtable<String, Object> htblColNameValue = new Hashtable( );
//        htblColNameValue.put("id", 1);
//        htblColNameValue.put("name", new String("Ahmed Noor" ) );
//        htblColNameValue.put("gpa", 0.95);
//        dbApp.insertIntoTable("Student", htblColNameValue);
//
//        htblColNameValue = new Hashtable( );
//        htblColNameValue.put("id", 3);
//        htblColNameValue.put("name", new String("Ahmed Omar" ) );
//        htblColNameValue.put("gpa", 1.95);
//        dbApp.insertIntoTable("Student", htblColNameValue);
//
//        htblColNameValue = new Hashtable( );
//        htblColNameValue.put("id", 2);
//        htblColNameValue.put("name", new String("Ahmed Ali" ) );
//        htblColNameValue.put("gpa", 2.95);
//        dbApp.insertIntoTable("Student", htblColNameValue);
//
//        htblColNameValue = new Hashtable( );
//        htblColNameValue.put("id", 5);
//        htblColNameValue.put("name", new String("Sayed" ) );
//        htblColNameValue.put("gpa", 3.95);
//        dbApp.insertIntoTable("Student", htblColNameValue);
//
//        htblColNameValue = new Hashtable( );
//        htblColNameValue.put("id", 4);
//        htblColNameValue.put("name", new String("Logine" ) );
//        htblColNameValue.put("gpa", 4.95);
//        dbApp.insertIntoTable("Student", htblColNameValue);


//        for (int i = 0; i < 3; i++) {
//            Page p;
//            p = Serializer.deserializePage("Student", i);
//            System.out.println(p.getPage());
//            Serializer.serializePage(p, "Student", i);
//        }

        dbApp.printTable("Student");

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
            throws DBAppException, IOException, ClassNotFoundException {
        Table t = Serializer.deserializeTable(strTableName);
        t.insertTuple(htblColNameValue);
        Serializer.serializeTable(t, strTableName);
    }

    public void updateTable(String strTableName,
                            String strClusteringKeyValue,
                            Hashtable<String,Object> htblColNameValue )
            throws DBAppException, IOException, ClassNotFoundException {
        Table t = Serializer.deserializeTable(strTableName);
        t.updateTuple(htblColNameValue);
        Serializer.serializeTable(t, strTableName);
    }

    public void printTable(String strTableName) throws DBAppException, IOException, ClassNotFoundException {

        File folder = new File("Serialized Database/" + strTableName);
        int fileCount = folder.listFiles().length;

        for(int i = 0; i < fileCount; i++) {
            Page p;
            p = Serializer.deserializePage("Student", i);
            System.out.println(p.getPage());
            Serializer.serializePage(p, "Student", i);
        }

    }

//    public Iterator selectFromTable(SQLTerm[] arrSQLTerms,
//                                    String[] strarrOperators)
//            throws DBAppException {
//
//    }

}
