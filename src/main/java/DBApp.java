import exceptions.DBAppException;
import helper_classes.AllRecordInIndexInserter;
import helper_classes.IndexNameGetter;
import helper_classes.SQLTerm;
import helper_classes.Serializer;
import index.Octree;
import tables.MyIterator;
import tables.Page;
import tables.Table;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class DBApp {

    static final String directoryPathResourcesData = "src/main/resources/Data/";

    public DBApp() {
        this.init();
    }

    public static void main(String[] args) throws DBAppException, IOException, ParseException {

//        testCreateStudentTable();

//        testInsertInStudentTable();

//        testUpdateStudentTable();

//        testDeleteFromStudentTable();

//        testPrintTableStudent();

//        testSQLTerm();

//        testCreateIndex();

//        testPrintIndex();
/*
        DBApp dbApp = new DBApp();

        String tableName = "students";

        Hashtable htblColNameType = new Hashtable();
        htblColNameType.put("id", "java.lang.String");
        htblColNameType.put("name", "java.lang.String");
        htblColNameType.put("gpa", "java.lang.double");
        htblColNameType.put("zage", "java.lang.Integer");
        htblColNameType.put("salary", "java.lang.double");
        htblColNameType.put("dob", "java.util.Date");

        Hashtable<String, String> htblColNameMin = new Hashtable<>();
        htblColNameMin.put("id", "40-0000");
        htblColNameMin.put("name", "aaaaaaaaaaa");
        htblColNameMin.put("gpa", "0.7");
        htblColNameMin.put("zage", "0");
        htblColNameMin.put("salary", "1000.0");
        htblColNameMin.put("dob", "1900-01-01");

        Hashtable<String, String> htblColNameMax = new Hashtable<>();
        htblColNameMax.put("id", "61-9999");
        htblColNameMax.put("name", "zzzzzzzzzzz");
        htblColNameMax.put("gpa", "4.0");
        htblColNameMax.put("zage", "100");
        htblColNameMax.put("salary", "10000000.0");
        htblColNameMax.put("dob", "2023-05-12");

        dbApp.createTable(tableName, "id", htblColNameType, htblColNameMin, htblColNameMax);

        String table = "students";
        String[] index = {"zage", "gpa", "name"};
        dbApp.createIndex(table, index);

        BufferedReader studentsTable = new BufferedReader(new FileReader("src/main/resources/students_table.csv"));
        String record;
        int limit = 20;
        int c = limit;
        if (limit == -1) {
            c = 1;
        }

        Hashtable<String, Object> row = new Hashtable<>();
        while ((record = studentsTable.readLine()) != null && c > 0) {
            String[] fields = record.split(",");

            if(c == 16) {
                row.put("id", fields[0]);
                row.put("salary", Double.parseDouble(fields[4]));

                String pattern = "yyyy-MM-dd";
                SimpleDateFormat formatter = new SimpleDateFormat(pattern);

                Date dob = formatter.parse(fields[5]);
                row.put("dob", dob);
                c--;
                dbApp.insertIntoTable("students", row);
                row.clear();
                continue;
            }
            if (c == 10) {
                row.put("id", fields[0]);
                row.put("name", fields[1]);
                row.put("zage", Integer.parseInt(fields[3]));
                row.put("salary", Double.parseDouble(fields[4]));

                String pattern = "yyyy-MM-dd";
                SimpleDateFormat formatter = new SimpleDateFormat(pattern);

                Date dob = formatter.parse(fields[5]);
                row.put("dob", dob);
                c--;
                dbApp.insertIntoTable("students", row);
                row.clear();
                continue;
            }
            if (c == 9) {
                row.put("id", fields[0]);
                row.put("zage", Integer.parseInt(fields[3]));
                row.put("salary", Double.parseDouble(fields[4]));

                String pattern = "yyyy-MM-dd";
                SimpleDateFormat formatter = new SimpleDateFormat(pattern);

                Date dob = formatter.parse(fields[5]);
                row.put("dob", dob);
                c--;
                dbApp.insertIntoTable("students", row);
                row.clear();
                continue;
            }
            if (c == 4) {
                row.put("id", fields[0]);
                row.put("name", fields[1]);
                row.put("zage", Integer.parseInt(fields[3]));
                row.put("salary", Double.parseDouble(fields[4]));

                String pattern = "yyyy-MM-dd";
                SimpleDateFormat formatter = new SimpleDateFormat(pattern);

                Date dob = formatter.parse(fields[5]);
                row.put("dob", dob);
                c--;
                dbApp.insertIntoTable("students", row);
                row.clear();
                continue;
            }

            row.put("id", fields[0]);
            row.put("name", fields[1]);
            row.put("gpa", Double.parseDouble(fields[2]));
            row.put("zage", Integer.parseInt(fields[3]));
            row.put("salary", Double.parseDouble(fields[4]));

            String pattern = "yyyy-MM-dd";
            SimpleDateFormat formatter = new SimpleDateFormat(pattern);

            Date dob = formatter.parse(fields[5]);
            row.put("dob", dob);

            dbApp.insertIntoTable("students", row);
            row.clear();
            if (limit != -1) {
                c--;
            }
        }
        studentsTable.close();

        dbApp.printIndex(table, index);
        dbApp.printTable(table);
*/
    }

    public static void testCreateStudentTable() throws DBAppException, IOException, ParseException {

        Hashtable htblColNameType = new Hashtable();
        htblColNameType.put("id", "java.lang.String");
        htblColNameType.put("name", "java.lang.String");
        htblColNameType.put("gpa", "java.lang.double");
        htblColNameType.put("zage", "java.lang.Integer");
        htblColNameType.put("salary", "java.lang.double");
        htblColNameType.put("dob", "java.util.Date");

        Hashtable<String, String> htblColNameMin = new Hashtable<>();
        htblColNameMin.put("id", "43-0000");
        htblColNameMin.put("name", "aaaaaaaaaaa");
        htblColNameMin.put("gpa", "0.7");
        htblColNameMin.put("zage", "0");
        htblColNameMin.put("salary", "1000.0");
        htblColNameMin.put("dob", "1900-01-01");

        Hashtable<String, String> htblColNameMax = new Hashtable<>();
        htblColNameMax.put("id", "99-9999");
        htblColNameMax.put("name", "zzzzzzzzzzz");
        htblColNameMax.put("gpa", "4.0");
        htblColNameMax.put("zage", "100");
        htblColNameMax.put("salary", "10000000.0");
        htblColNameMax.put("dob", "2023-05-12");

        DBApp dbApp = new DBApp();
        dbApp.createTable("Student", "id", htblColNameType,
                htblColNameMin, htblColNameMax);

    }

    public static void testInsertInStudentTable() throws DBAppException, ParseException {
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
1 Abdo 0.96 20 25000.0 2002-09-01
3 Sayed 1.8 36 30000.0 1987-03-10
2 Omar 1.75 20 2500.0 2002-29-11
7 Ahmed 0.87 20 1836.57 2002-08-04
5 Ali 0.97 20 36520.25 2002-10-01
6 Neymar 2.0 33 220000.0 1989-09-01
4 Abdo 2.0 20 2430.02 2002-09-01
9 Abdo 2.0 20 1442.025 2002-09-01
8 Ibrahim 2.5 57 65000.0 1965-08-23
10 Abdo 0.7 20 6525.0 2002-09-01
11 Logine 0.90 21 25200.50 2002-03-20

1
52-4094 Ahmed 0.7 100 1836.57 1923-08-04

*/


        Hashtable<String, Object> htblColNameValue;

        Scanner sc = new Scanner(System.in);

        DBApp dbApp = new DBApp();

        int n = sc.nextInt();
        for (int i = 0; i < n; i++) {
            htblColNameValue = new Hashtable();
            String id = sc.next();
            String name = sc.next();
            double gpa = sc.nextDouble();
            int age = sc.nextInt();
            double salary = sc.nextDouble();
            String pattern = "yyyy-MM-dd";
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
            Date date = simpleDateFormat.parse(sc.next());

            htblColNameValue.put("id", id);
            htblColNameValue.put("name", name);
            htblColNameValue.put("gpa", gpa);
            htblColNameValue.put("zage", age);
            htblColNameValue.put("salary", salary);
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
//        htblColNameValue.put("gpa", 0.7);
//        htblColNameValue.put("id", 8);
//        htblColNameValue.put("name", "AbDo");
//        htblColNameValue.put("age", 20);
//        htblColNameValue.put("salary", 6525.0);
        DBApp dbApp = new DBApp();
        dbApp.deleteFromTable("Student", htblColNameValue);
    }

    public static void testSQLTerm() throws DBAppException {

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

    public static void testCreateIndex() throws DBAppException {
        DBApp dbApp = new DBApp();
        String[] strarrColNames = {"zage", "name", "gpa"};
        dbApp.createIndex("Student", strarrColNames);
        strarrColNames = new String[]{"id", "salary", "dob"};
        dbApp.createIndex("Student", strarrColNames);

    }

    public static void testPrintIndex() throws DBAppException {
        DBApp dbApp = new DBApp();
        String[] strarrColNames = {"zage", "name", "gpa"};
        dbApp.printIndex("Student", strarrColNames);
        System.out.println();
//        strarrColNames = new String[]{"id", "salary", "dob"};
//        dbApp.printIndex("Student", strarrColNames);

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
//            System.out.println(ot.getRoot().toString());
        } catch (Exception e) {
            throw new DBAppException(e.getMessage());
        }
    }

}
