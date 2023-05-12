import exceptions.DBAppException;
import helper_classes.AllRecordInIndexInserter;
import helper_classes.IndexNameGetter;
import helper_classes.SQLTerm;
import helper_classes.Serializer;
import index.Octree;
import tables.MyIterator;
import tables.Page;
import tables.Table;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class DBApp {

    static final String directoryPathResourcesData = "src/main/resources/Data/";

    public DBApp() {
        this.init();
    }

    public static void main(String[] args) throws DBAppException, IOException, ParseException, ClassNotFoundException {

//        testCreateStudentTable();

//        testInsertInStudentTable();

//        testUpdateStudentTable();

//        testDeleteFromStudentTable();

//        testPrintTableStudent();

//        testSQLTerm();

//        testCreateIndex();

//        testPrintIndex();

    }

    public static void testCreateStudentTable() throws DBAppException, IOException, ParseException {

        Hashtable htblColNameType = new Hashtable();
        htblColNameType.put("id", "java.lang.Integer");
        htblColNameType.put("name", "java.lang.String");
        htblColNameType.put("gpa", "java.lang.double");
        htblColNameType.put("dob", "java.util.Date");

        Hashtable<String, String> htblColNameMin = new Hashtable<>();
        htblColNameMin.put("id", "1");
        htblColNameMin.put("name", "aaaaaaaaaaa");
        htblColNameMin.put("gpa", "0.01");
        htblColNameMin.put("dob", "1900-01-01");

        Hashtable<String, String> htblColNameMax = new Hashtable<>();
        htblColNameMax.put("id", "32");
        htblColNameMax.put("name", "zzzzzzzzzzz");
        htblColNameMax.put("gpa", "10.0");
        htblColNameMax.put("dob", "2023-05-12");

        DBApp dbApp = new DBApp();
        dbApp.createTable("Student", "id", htblColNameType,
                htblColNameMin, htblColNameMax);

    }

    public static void testInsertInStudentTable() throws DBAppException, IOException, ParseException, ClassNotFoundException {
/*
        7
        1 AhmedNoor 0.95
        3 AhmedOmar 1.95
        2 AhmedAli 2.95
        5 Sayed 3.95
        4 Logine 4.95
        7 Omar 4.0
        11 Abdelrahman 0.9

        7
1 AHMEDNOOR 0.95
3 AHMEDOMAR 1.95
5 SAYED 3.95
2 AHMEDALI 2.95
6
7 OMAR 4.0
4 LOGINE 4.95
11 ZIAD 0.9
10 ABDELRAHMAN 1.2


11
1 Abdo 2.0 2002-09-01
3 Sayed 1.8 1987-03-10
2 Abdo 2.0 2002-09-01
7 Abdo 2.0 2002-09-01
5 Abdo 2.0 2002-09-01
6 Abdo 2.0 2002-09-01
4 Abdo 2.0 2002-09-01
9 Abdo 2.0 2002-09-01
8 Ibrahim 2.5 1965-08-23
10 Abdo 2.0 2002-09-01
11 Logine 1.8 2021-07-20

*/


        Hashtable<String, Object> htblColNameValue;

        Scanner sc = new Scanner(System.in);

        DBApp dbApp = new DBApp();

        int n = sc.nextInt();
        for (int i = 0; i < n; i++) {
            htblColNameValue = new Hashtable();
            int id = sc.nextInt();
            String name = sc.next();
            double gpa = sc.nextDouble();
            String pattern = "yyyy-MM-dd";
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
            Date date = simpleDateFormat.parse(sc.next());

            htblColNameValue.put("id", id);
            htblColNameValue.put("name", name);
            htblColNameValue.put("gpa", gpa);
            htblColNameValue.put("dob", date);
            dbApp.insertIntoTable("Student", htblColNameValue);
        }

    }

    public static void testUpdateStudentTable() throws DBAppException {
        Hashtable<String, Object> htblColNameValue = new Hashtable();
        htblColNameValue.put("gpa", 0.1);
        DBApp dbApp = new DBApp();
        dbApp.updateTable("Student", "8", htblColNameValue);
    }

    public static void testDeleteFromStudentTable() throws DBAppException {
        Hashtable<String, Object> htblColNameValue = new Hashtable();
//        htblColNameValue.put("gpa", 2.0);
//        htblColNameValue.put("id", 8);
//        htblColNameValue.put("name", "gogo");
        DBApp dbApp = new DBApp();
        dbApp.deleteFromTable("Student", htblColNameValue);
    }

    public static void testSQLTerm() throws DBAppException, IOException, ParseException, ClassNotFoundException {

        DBApp dbApp = new DBApp();
        Scanner sc = new Scanner(System.in);
        int n = sc.nextInt();
        SQLTerm[] arrSQLTerms = new SQLTerm[n];
        for (int i = 0; i < n; i++) {
            arrSQLTerms[i] = new SQLTerm();
            arrSQLTerms[i]._strTableName = "Student";
            arrSQLTerms[i]._strColumnName = sc.next();
            arrSQLTerms[i]._strOperator = sc.next();
            int type = sc.nextInt();  //1 --> int, 2 --> double, 3 --> string, 4 --> date
            String s = sc.next();
            switch (type) {
                case 1:
                    arrSQLTerms[i]._objValue = Integer.parseInt(s);
                    break;
                case 2:
                    arrSQLTerms[i]._objValue = Double.parseDouble(s);
                    break;
                case 3:
                    arrSQLTerms[i]._objValue = s;
                    break;
                case 4:
                    String pattern = "yyyy-MM-dd";
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
                    try {
                        arrSQLTerms[i]._objValue = simpleDateFormat.parse(s);
                    } catch (ParseException e) {
                        throw new DBAppException("Invalid date format");
                    }
                    break;
            }
        }
        String[] strarrOperators = new String[n - 1];
        for (int i = 0; i < n - 1; i++) {
            strarrOperators[i] = sc.next();
        }

        Iterator resultSet = dbApp.selectFromTable(arrSQLTerms, strarrOperators);
        System.out.println("{");
        while (resultSet.hasNext()) {
            System.out.println(resultSet.next());
        }
        System.out.println("}");

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
        4
        gpa >= 2 1.5
        name = 3 Abdo
        name != 3 Ali
        dob >= 4 2002-08-04
        AND
        AND
        aNd
        -----------------
 */

    }

    public static void testCreateIndex() throws DBAppException, IOException, ParseException, ClassNotFoundException {
        DBApp dbApp = new DBApp();
        String[] strarrColNames = {"dob", "name", "gpa"};
        dbApp.createIndex("Student", strarrColNames);
    }

    public static void testPrintIndex() throws DBAppException, IOException, ParseException, ClassNotFoundException {
        DBApp dbApp = new DBApp();
        String[] strarrColNames = {"dob", "name", "gpa"};
        dbApp.printIndex("Student", strarrColNames);
    }

    public static void testPrintTableStudent() throws DBAppException {
        DBApp dbApp = new DBApp();
        dbApp.printTable("Student");
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
            throws DBAppException, IOException, ParseException {

        try {
            strTableName = strTableName.toLowerCase();
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
            throws DBAppException, IOException, ClassNotFoundException, ParseException {
//        try {
        strTableName = strTableName.toLowerCase();
        Table t = Serializer.deserializeTable(strTableName);
        t.insertTuple(htblColNameValue);
        Serializer.serializeTable(t, strTableName);
//        } catch (Exception e) {
//            throw new DBAppException(e.getMessage());
//        }
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
            throws DBAppException, IOException, ParseException, ClassNotFoundException {

//        try {
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
//        } catch (Exception e) {
//            throw new DBAppException(e.getMessage());
////            return null;
//        }

    }


    public void createIndex(String strTableName,
                            String[] strarrColName) throws DBAppException, IOException, ClassNotFoundException, ParseException {
//        try {
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
//        } catch (Exception e) {
//            throw new DBAppException(e.getMessage());
//        }

    }

    public void printIndex(String strTableName, String[] strarrColName) throws IOException, DBAppException, ParseException, ClassNotFoundException {
        strTableName = strTableName.toLowerCase();
        Arrays.sort(strarrColName);
        String indexName = IndexNameGetter.getIndexName(strarrColName);
        Table t = Serializer.deserializeTable(strTableName);
        Octree ot = Serializer.deserializeIndex(t, indexName);
        ot.printIndexDFS();
    }

}
