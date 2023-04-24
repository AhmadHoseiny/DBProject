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

//        Hashtable htblColNameType = new Hashtable( );
//        htblColNameType.put("id", "java.lang.Integer");
//        htblColNameType.put("name", "java.lang.String");
//        htblColNameType.put("gpa", "java.lang.double");
//
//        Hashtable<String, String> htblColNameMin = new Hashtable<>();
//        htblColNameMin.put("id", "1");
//        htblColNameMin.put("name", "A");
//        htblColNameMin.put("gpa", "0.01");
//
//        Hashtable<String, String> htblColNameMax = new Hashtable<>();
//        htblColNameMax.put("id", "999");
//        htblColNameMax.put("name", "zzzzzzzzzzzzzzzzzzzzz");
//        htblColNameMax.put("gpa", "6.0");



        DBApp dbApp = new DBApp();
//        dbApp.createTable("Student", "id", htblColNameType,
//                htblColNameMin, htblColNameMax);

/*
        7
        1 AhmedNoor 0.95
        3 AhmedOmar 1.95
        2 AhmedAli 2.95
        5 Sayed 3.95
        4 Logine 4.95
        7 Omar 4.0
        11 Abdelrahman 0.9
*/

        Hashtable<String, Object> htblColNameValue;

//        Scanner sc = new Scanner(System.in);
//
//        int n = sc.nextInt();
//        for(int i=0 ; i<n ; i++){
//            htblColNameValue = new Hashtable( );
//            int id = sc.nextInt();
//            String name = sc.next();
//            double gpa = sc.nextDouble();
//            htblColNameValue.put("id", id);
//            htblColNameValue.put("name", name);
//            htblColNameValue.put("gpa", gpa);
//            dbApp.insertIntoTable("Student", htblColNameValue);
//        }

//        htblColNameValue = new Hashtable( );
//        htblColNameValue.put("gpas", 0.95);
//        dbApp.updateTable("Student", "4", htblColNameValue);

//        htblColNameValue = new Hashtable( );
//        dbApp.deleteFromTable("Student", htblColNameValue);
//
//        htblColNameValue = new Hashtable( );
//        htblColNameValue.put("gpas", "0.08");
//        dbApp.deleteFromTable("Student", htblColNameValue);


        dbApp.printTable("Student");

//
        Scanner sc = new Scanner(System.in);
        int n = sc.nextInt();
        SQLTerm[] arrSQLTerms = new SQLTerm[n];
        for(int i=0 ; i<n ; i++){
            arrSQLTerms[i] = new SQLTerm();
            arrSQLTerms[i]._strTableName = "Student";
            arrSQLTerms[i]._strColumnName= sc.next();
            arrSQLTerms[i]._strOperator = sc.next();
            int type = sc.nextInt();  //1 --> int, 2 --> double, 3 --> string
            String s = sc.next();
            switch (type){
                case 1:
                    arrSQLTerms[i]._objValue = Integer.parseInt(s);
                    break;
                case 2:
                    arrSQLTerms[i]._objValue = Double.parseDouble(s);
                    break;
                case 3:
                    arrSQLTerms[i]._objValue = s;
                    break;
            }
        }
        String[]strarrOperators = new String[n-1];
        for(int i=0 ; i<n-1 ; i++){
            strarrOperators[i] = sc.next();
        }

        Iterator resultSet = dbApp.selectFromTable(arrSQLTerms, strarrOperators);
        System.out.println("{");
        while(resultSet.hasNext()){
            System.out.println(resultSet.next());
        }
        System.out.println("}");
//
/*
        3
        id > 1 2
        name = 3 AhmedNoor
        gpa < 2 0.95
        AND
        OR

        -----------------
        4
        id >= 1 4
        gpa > 2 1.0
        gpa < 2 4.0
        id <= 1 7
        AND
        OR
        AND
        -----------------
 */
//        Scanner sc = new Scanner(System.in);
//        int n = sc.nextInt();
//        SQLTerm[] arrSQLTerms = new SQLTerm[n];
//        for(int i=0 ; i<n ; i++){
//            arrSQLTerms[i] = new SQLTerm();
//            arrSQLTerms[i]._strTableName = "Student";
//            arrSQLTerms[i]._strColumnName= sc.next();
//            arrSQLTerms[i]._strOperator = sc.next();
//            int type = sc.nextInt();  //1 --> int, 2 --> double, 3 --> string
//            String s = sc.next();
//            switch (type){
//                case 1:
//                    arrSQLTerms[i]._objValue = Integer.parseInt(s);
//                    break;
//                case 2:
//                    arrSQLTerms[i]._objValue = Double.parseDouble(s);
//                    break;
//                case 3:
//                    arrSQLTerms[i]._objValue = s;
//                    break;
//            }
//        }
//        String[]strarrOperators = new String[n-1];
//        for(int i=0 ; i<n-1 ; i++){
//            strarrOperators[i] = sc.next();
//        }
//
//        Iterator resultSet = dbApp.selectFromTable(arrSQLTerms, strarrOperators);
//        System.out.println("{");
//        while(resultSet.hasNext()){
//            System.out.println(resultSet.next());
//        }
//        System.out.println("}");

    }

    public void createTable(String strTableName,
                            String strClusteringKeyColumn,
                            Hashtable<String,String> htblColNameType,
                            Hashtable<String,String> htblColNameMin,
                            Hashtable<String,String> htblColNameMax )
            throws DBAppException {

        try {
            Table t = new Table(strTableName, strClusteringKeyColumn, htblColNameType, htblColNameMin, htblColNameMax);
            Serializer.serializeTable(t, strTableName);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

    }

    public void insertIntoTable(String strTableName,
                                Hashtable<String,Object> htblColNameValue)
            throws DBAppException {
        try {
            Table t = Serializer.deserializeTable(strTableName);
            t.insertTuple(htblColNameValue);
            Serializer.serializeTable(t, strTableName);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public void updateTable(String strTableName,
                            String strClusteringKeyValue,
                            Hashtable<String,Object> htblColNameValue )
            throws DBAppException {
        try {
            Table t = Serializer.deserializeTable(strTableName);
            t.updateTuple(strClusteringKeyValue, htblColNameValue);
            Serializer.serializeTable(t, strTableName);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public void deleteFromTable(String strTableName,
                                Hashtable<String,Object> htblColNameValue)
            throws DBAppException {
        try {
            Table t = Serializer.deserializeTable(strTableName);
            t.deleteTuple(htblColNameValue);
            Serializer.serializeTable(t, strTableName);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
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

    public Iterator selectFromTable(SQLTerm[] arrSQLTerms,
                                    String[] strarrOperators)
            throws DBAppException {

        try{
            MyIterator it = new MyIterator(arrSQLTerms, strarrOperators);
            return it;
        }
        catch (Exception e){
            System.out.println(e.getMessage());
            return null;
        }


    }



}
