import java.io.*;
import java.util.*;

import exceptions.*;
import helper_classes.*;
import tables.*;

public class DBApp {

    public DBApp() {
        this.init();
    }

    public void init() {
        String directoryPath = "Serialized Database/";
        File directory = new File(directoryPath);
        if(!directory.isDirectory())
            new File(directoryPath).mkdirs();
    }

    public static void main(String[] args) throws IOException, DBAppException, ClassNotFoundException {

        Hashtable htblColNameType = new Hashtable( );
        htblColNameType.put("id", "java.lang.String");
        htblColNameType.put("name", "java.lang.String");
        htblColNameType.put("gpa", "java.lang.double");

        Hashtable<String, String> htblColNameMin = new Hashtable<>();
        htblColNameMin.put("id", "1");
        htblColNameMin.put("name", "A");
        htblColNameMin.put("gpa", "0.01");

        Hashtable<String, String> htblColNameMax = new Hashtable<>();
        htblColNameMax.put("id", "999");
        htblColNameMax.put("name", "zzzzzzzzzzzzzzzzzzzzz");
        htblColNameMax.put("gpa", "6.0");



          DBApp dbApp = new DBApp();
       // dbApp.createTable("Student", "id", htblColNameType,
         //       htblColNameMin, htblColNameMax);

/*
        6
        1 AhmedNoor 0.95
        3 AhmedOmar 1.95
        2 AhmedAli 2.95
        5 Sayed 3.95
        4 Logine 4.95
        7 Omar 4.0
*/
//        Scanner sc = new Scanner(System.in);
//
//        int n = sc.nextInt();
//        for(int i=0 ; i<n ; i++){
//            Hashtable<String, Object> htblColNameValue = new Hashtable( );
//            String id = sc.next();
//            String name = sc.next();
//            double gpa = sc.nextDouble();
//            htblColNameValue.put("id", id);
//            htblColNameValue.put("name", name);
//            htblColNameValue.put("gpa", gpa);
//            dbApp.insertIntoTable("Student", htblColNameValue);
//        }

//        Hashtable<String, Object> htblColNameValue = new Hashtable( );
//        htblColNameValue.put("id", "11");
//        htblColNameValue.put("names", "abdelrahman");
//        htblColNameValue.put("gpa", 0.9);
//        dbApp.insertIntoTable("Student", htblColNameValue);


        Hashtable htblColNameValue;
//        htblColNameValue = new Hashtable( );
//        htblColNameValue.put("gpas", 0.95);
//        dbApp.updateTable("Student", "4", htblColNameValue);

        htblColNameValue = new Hashtable( );
        dbApp.deleteFromTable("Student", htblColNameValue);
//
//        htblColNameValue = new Hashtable( );
//        htblColNameValue.put("gpas", "0.08");
//        dbApp.deleteFromTable("Student", htblColNameValue);


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
        try {
            t.insertTuple(htblColNameValue);
        } catch (DBAppException e) {
            System.out.println(e.getMessage());
        }
        Serializer.serializeTable(t, strTableName);
    }

    public void updateTable(String strTableName,
                            String strClusteringKeyValue,
                            Hashtable<String,Object> htblColNameValue )
            throws DBAppException, IOException, ClassNotFoundException {
        Table t = Serializer.deserializeTable(strTableName);
        try {
            t.updateTuple(strClusteringKeyValue, htblColNameValue);
        } catch (DBAppException e) {
            System.out.println(e.getMessage());
        }
        Serializer.serializeTable(t, strTableName);
    }

    public void deleteFromTable(String strTableName,
                                Hashtable<String,Object> htblColNameValue)
            throws DBAppException, IOException, ClassNotFoundException {
        Table t = Serializer.deserializeTable(strTableName);
        try {
            t.deleteTuple(htblColNameValue);
        } catch (DBAppException e) {
            System.out.println(e.getMessage());
        }
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
