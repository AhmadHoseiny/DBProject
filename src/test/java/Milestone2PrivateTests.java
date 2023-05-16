import exceptions.DBAppException;
import helper_classes.SQLTerm;
import org.junit.jupiter.api.*;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;


@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class Milestone2PrivateTests {

    static final String dataDirPath = "src/main/resources/Data/";
    static final String configFilePath = "src/main/resources/DBApp.config";
    static final String metaFilePath = "src/main/resources/metadata.csv";

//    @Test
//    @Order(1)
//    public void testSetPageSize() throws Exception {
//        File configDir = new File(configFilePath);
//
//        if (!configDir.exists()) {
//            throw new DBAppException("`DBApp.config` does not exist");
//        }
//
//        Path path = Paths.get(configFilePath);
//        List<String> config = Files.readAllLines(path);
//        boolean lineFound = false;
//        for (int i = 0; i < config.size(); i++) {
//            if (config.get(i).toLowerCase().contains("page")) {
//                config.set(i, config.get(i).replaceAll("\\d+", "250"));
//                lineFound = true;
//                break;
//            }
//        }
//
//        if (!lineFound) {
//            throw new DBAppException("Cannot set page size, make sure that key `MaximumRowsCountInTablePage` is present in DBApp.config");
//        }
//
//        Files.write(path, config);
//
//    }

    @Test
    @Order(1)
    public void testClearMetaDataFile() throws Exception {

        File metaFile = new File(metaFilePath);

        if (!metaFile.exists()) {
            throw new Exception("`metadata.csv` in Resources folder does not exist");
        }

        PrintWriter writer = new PrintWriter(metaFile);
        writer.write("");
        writer.close();
    }

    @Test
    @Order(2)
    public void testDataDirectory() throws Exception {
        DBApp dbApp = new DBApp();
        dbApp.init();

        File dataDir = new File(dataDirPath);

        if (!dataDir.isDirectory() || !dataDir.exists()) {
            throw new Exception("`data` Directory in Resources folder does not exist");
        }

        ArrayList<String> files = new ArrayList<>();
        try {
            files = Files.walk(Paths.get(dataDirPath))
                    .map(f -> f.toAbsolutePath().toString())
                    .filter(p -> !Files.isDirectory(Paths.get(p)))
                    .collect(Collectors.toCollection(ArrayList::new));
        } catch (IOException e) {
            e.printStackTrace();
        }

//        System.out.println(files);
        for (String file : files) {
            Files.delete(Paths.get(file));
        }
    }

    @Test
    @Order(3)
    public void testTableCreation() throws Exception {
        DBApp dbApp = new DBApp();
        dbApp.init();

        createStudentTable(dbApp);
        createCoursesTable(dbApp);
        createTranscriptsTable(dbApp);
        createPCsTable(dbApp);

        dbApp = null;
    }

    @Test
    @Order(4)
    public void testCreateStudentsAgeGpaNameIndex() throws Exception {
        DBApp dbApp = new DBApp();
        dbApp.init();
        String table = "students";
        String[] index = {"age", "gpa", "name"};
        dbApp.createIndex(table, index);
        dbApp = null;
    }

    @Test
    @Order(5)
    public void testRecordInsertions() throws Exception {
        DBApp dbApp = new DBApp();
        dbApp.init();
        int limit = 20;

        insertStudentRecords(dbApp, limit);
        insertPCsRecords(dbApp, limit);
        insertTranscriptsRecords(dbApp, limit);
        insertCoursesRecords(dbApp, limit);
        dbApp = null;
    }

    @Test
    @Order(6)
    public void testCreateTranscriptsCourseGpaIdIndex() throws Exception {
        DBApp dbApp = new DBApp();
        dbApp.init();
        String table = "transcripts";
        String[] index = {"gpa", "student_id", "course_name"};
        dbApp.createIndex(table, index);
        dbApp = null;
    }

    @Test
    @Order(7)
    public void testCreateTranscriptsIndex() throws Exception {
        DBApp dbApp = new DBApp();
        dbApp.init();
        String table = "transcripts";
        String[] index = {"student_id", "course_name", "date_passed"};
        DBApp finalDbApp = dbApp;
        Assertions.assertThrows(DBAppException.class, () -> finalDbApp.createIndex(table, index));
        dbApp = null;
    }

    @Test
    @Order(8)
    public void testCreateIntegerIndex() throws Exception {
        DBApp dbApp = new DBApp();
        dbApp.init();
        String table = "pcs";
        String[] index = {"pc_id"};
//        dbApp.createIndex(table, index);
        DBApp finalDbApp = dbApp;
        Assertions.assertThrows(DBAppException.class, () -> finalDbApp.createIndex(table, index));
        dbApp = null;
    }

    @Test
    @Order(9)
    public void testCreateStudentsDobIdSalaryIndex() throws Exception {
        DBApp dbApp = new DBApp();
        dbApp.init();
        String table = "students";
        String[] index = {"salary", "dob", "id"};
        dbApp.createIndex(table, index);
        dbApp = null;
    }

    @Test
    @Order(10)
    public void testSelectEmptyStudents() throws Exception{
        // Should return an empty iterator with no errors thrown

        SQLTerm[] arrSQLTerms;
        arrSQLTerms = new SQLTerm[2];
        arrSQLTerms[0] = new SQLTerm();
        arrSQLTerms[0]._strTableName = "students";
        arrSQLTerms[0]._strColumnName= "name";
        arrSQLTerms[0]._strOperator = "=";
        arrSQLTerms[0]._objValue ="John";

        arrSQLTerms[1] = new SQLTerm();
        arrSQLTerms[1]._strTableName = "students";
        arrSQLTerms[1]._strColumnName= "gpa";
        arrSQLTerms[1]._strOperator = "=";
        arrSQLTerms[1]._objValue = 0.7;

        String[]strarrOperators = new String[1];
        strarrOperators[0] = "AND";

        DBApp dbApp = new DBApp();
        dbApp.init();
        Iterator resultSet = dbApp.selectFromTable(arrSQLTerms , strarrOperators);

        boolean entered =  false;
        if (resultSet.hasNext())
            entered = true;

        if(entered)
            throw new DBAppException();

    }

    @Test
    @Order(11)
    public void testSelectActualStudentOR() throws Exception{
        // Should return a non-empty iterator with no errors thrown

        BufferedReader studentsTable = new BufferedReader(new FileReader("src/main/resources/students_table.csv"));
        String record;
        int c = 0;
        int finalLine = 1;
        Hashtable<String, Object> row = new Hashtable();


        while ((record = studentsTable.readLine()) != null && c <= finalLine) {
            if (c == finalLine) {
                String[] fields = record.split(",");

                row.put("id", fields[0]);
                row.put("name", fields[1]);
                row.put("gpa", Double.parseDouble(fields[2]));
                row.put("age", Integer.parseInt(fields[3]));
                row.put("salary", Double.parseDouble(fields[4]));

                String pattern = "yyyy-MM-dd";
                SimpleDateFormat formatter = new SimpleDateFormat(pattern);

                Date dob = formatter.parse(fields[5]);
                row.put("dob", dob);

            }
            c++;
        }
        studentsTable.close();


        SQLTerm[] arrSQLTerms;
        arrSQLTerms = new SQLTerm[2];
        arrSQLTerms[0] = new SQLTerm();
        arrSQLTerms[0]._strTableName = "students";
        arrSQLTerms[0]._strColumnName= "name";
        arrSQLTerms[0]._strOperator = "=";
        arrSQLTerms[0]._objValue = (Comparable) row.get("name");

        arrSQLTerms[1] = new SQLTerm();
        arrSQLTerms[1]._strTableName = "students";
        arrSQLTerms[1]._strColumnName= "gpa";
        arrSQLTerms[1]._strOperator = "<=";
        arrSQLTerms[1]._objValue = (Comparable) row.get("gpa");

        String[]strarrOperators = new String[1];
        strarrOperators[0] = "OR";

        DBApp dbApp = new DBApp();
        dbApp.init();
        Iterator resultSet = dbApp.selectFromTable(arrSQLTerms , strarrOperators);

        System.out.println("Select Test");
        while (resultSet.hasNext()){
            System.out.println("Student OR "+resultSet.next());
        }
    }

    @Test
    @Order(12)
    public void testSelectActualStudentAND() throws Exception{
        // Should return a non-empty iterator with no errors thrown

        BufferedReader studentsTable = new BufferedReader(new FileReader("src/main/resources/students_table.csv"));
        String record;
        int c = 0;
        int finalLine = 1;
        Hashtable<String, Object> row = new Hashtable();


        while ((record = studentsTable.readLine()) != null && c <= finalLine) {
            if (c == finalLine) {
                String[] fields = record.split(",");
                row.put("id", fields[0]);
                row.put("name", fields[1]);
                row.put("gpa", Double.parseDouble(fields[2]));
                row.put("age", Integer.parseInt(fields[3]));
                row.put("salary", Double.parseDouble(fields[4]));

                String pattern = "yyyy-MM-dd";
                SimpleDateFormat formatter = new SimpleDateFormat(pattern);

                Date dob = formatter.parse(fields[5]);
                row.put("dob", dob);

            }
            c++;
        }
        studentsTable.close();


        SQLTerm[] arrSQLTerms;
        arrSQLTerms = new SQLTerm[2];
        arrSQLTerms[0] = new SQLTerm();
        arrSQLTerms[0]._strTableName = "students";
        arrSQLTerms[0]._strColumnName= "name";
        arrSQLTerms[0]._strOperator = "=";
        arrSQLTerms[0]._objValue = (Comparable) row.get("name");

        arrSQLTerms[1] = new SQLTerm();
        arrSQLTerms[1]._strTableName = "students";
        arrSQLTerms[1]._strColumnName= "gpa";
        arrSQLTerms[1]._strOperator = "=";
        arrSQLTerms[1]._objValue = (Comparable) row.get("gpa");

        String[]strarrOperators = new String[1];
        strarrOperators[0] = "AND";
// select * from Student where name = “John Noor” or gpa = 1.5;
        DBApp dbApp = new DBApp();
        dbApp.init();
        Iterator resultSet = dbApp.selectFromTable(arrSQLTerms , strarrOperators);

        System.out.println("Select Test");
        while (resultSet.hasNext()){
            System.out.println("Student AND "+resultSet.next());
        }

    }

    @Test
    @Order(13)
    public void testStudentsDeletionExtra() {
        final DBApp dbApp = new DBApp();
        dbApp.init();

        String table = "students";
        Hashtable<String, Object> row = new Hashtable();
        row.put("id", "82-8772");
        row.put("middle_name", "hamada");

        Assertions.assertThrows(DBAppException.class, () -> {
            dbApp.deleteFromTable(table, row);
        });
    }

    @Test
    @Order(14)
    public void testCoursesDeleteExtra() {
        final DBApp dbApp = new DBApp();
        dbApp.init();

        String table = "courses";
        Hashtable<String, Object> row = new Hashtable();
        row.put("course_name", "PFeaCY");
        row.put("students", 100);

        Assertions.assertThrows(DBAppException.class, () -> {
            dbApp.deleteFromTable(table, row);
        });
    }

    @Test
    @Order(15)
    public void testTranscriptsDeleteExtra() {
        final DBApp dbApp = new DBApp();
        dbApp.init();

        String table = "transcripts";
        Hashtable<String, Object> row = new Hashtable();

        row.put("course_name", "CMdzKv");
        row.put("elective", false);

        Assertions.assertThrows(DBAppException.class, () -> {
            dbApp.deleteFromTable(table, row);
        });
    }

    @Test
    @Order(16)
    public void testPCsDeleteExtra() {
        final DBApp dbApp = new DBApp();
        dbApp.init();

        String table = "pcs";
        Hashtable<String, Object> row = new Hashtable<>();
        row.put("student_id", "60-2513");
        row.put("os", "linux");

        Assertions.assertThrows(DBAppException.class, () -> {
            dbApp.deleteFromTable(table, row);
        });


    }

    @Test
    @Order(17)
    public void testMissingCoursesInsertion() throws ParseException {
        final DBApp dbApp = new DBApp();
        dbApp.init();

        String table = "courses";
        Hashtable<String, Object> row = new Hashtable();
        row.put("course_id", "foo");
        row.put("course_name", "bar");


        String pattern = "yyyy-MM-dd";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
        Date date_added = simpleDateFormat.parse("2011-04-01");
        row.put("date_added", date_added);


//        Assertions.assertThrows(DBAppException.class, () -> {
//                    dbApp.insertIntoTable(table, row);
//                }
//        );

    }

    @Test
    @Order(18)
    public void testMissingTranscriptsInsertion() throws ParseException {
        final DBApp dbApp = new DBApp();
        dbApp.init();

        String table = "transcripts";
        Hashtable<String, Object> row = new Hashtable();
        row.put("gpa", 1.5);

        row.put("course_name", "bar");


        String pattern = "yyyy-MM-dd";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
        Date date_passed = simpleDateFormat.parse("2011-04-01");
        row.put("date_passed", date_passed);


//        Assertions.assertThrows(DBAppException.class, () -> {
//                    dbApp.insertIntoTable(table, row);
//                }
//        );

    }


    @Test
    @Order(19)
    public void testMissingPCsInsertion() {
        final DBApp dbApp = new DBApp();
        dbApp.init();

        String table = "pcs";
        Hashtable<String, Object> row = new Hashtable();
        row.put("pc_id", 50);

//        Assertions.assertThrows(DBAppException.class, () -> {
//                    dbApp.insertIntoTable(table, row);
//                }
//        );
    }

    @Test
    @Order(20)
    public void testExtraStudentsInsertion() throws ParseException {
        final DBApp dbApp = new DBApp();
        dbApp.init();

        String table = "students";
        Hashtable<String, Object> row = new Hashtable();
        row.put("id", "31-1235");
        row.put("first_name", "foo");
        row.put("middle_name", "bateekh");
        row.put("last_name", "bar");


        String pattern = "yyyy-MM-dd";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
        Date dob = simpleDateFormat.parse("2011-04-01");
        row.put("dob", dob);

        row.put("gpa", 1.1);


        Assertions.assertThrows(DBAppException.class, () -> {
                    dbApp.insertIntoTable(table, row);
                }
        );

    }

    @Test
    @Order(21)
    public void testExtraCoursesInsertion() throws ParseException {
        final DBApp dbApp = new DBApp();
        dbApp.init();

        String table = "courses";
        Hashtable<String, Object> row = new Hashtable();

        String pattern = "yyyy-MM-dd";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
        Date date_added = simpleDateFormat.parse("2011-04-01");
        row.put("date_added", date_added);

        row.put("course_id", "foo");
        row.put("course_name", "bar");
        row.put("hours", 13);
        row.put("semester", 5);

        Assertions.assertThrows(DBAppException.class, () -> {
                    dbApp.insertIntoTable(table, row);
                }
        );

    }

    @Test
    @Order(22)
    public void testExtraTranscriptsInsertion() throws ParseException {
        final DBApp dbApp = new DBApp();
        dbApp.init();

        String table = "transcripts";
        Hashtable<String, Object> row = new Hashtable();
        row.put("gpa", 1.5);
        row.put("student_id", "34-9874");
        row.put("course_name", "bar");
        row.put("elective", true);


        String pattern = "yyyy-MM-dd";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
        Date date_passed = simpleDateFormat.parse("2011-04-01");
        row.put("date_passed", date_passed);


        Assertions.assertThrows(DBAppException.class, () -> {
                    dbApp.insertIntoTable(table, row);
                }
        );
    }

    @Test
    @Order(23)
    public void testExtraPCsInsertion() {
        final DBApp dbApp = new DBApp();
        dbApp.init();

        String table = "pcs";
        Hashtable<String, Object> row = new Hashtable();
        row.put("pc_id", 50);
        row.put("student_id", "31-12121");
        row.put("room", "C7.02");

        Assertions.assertThrows(DBAppException.class, () -> {
                    dbApp.insertIntoTable(table, row);
                }
        );
    }

    @Test
    @Order(24)
    public void testUpdateStudents() throws Exception {
        DBApp dbApp = new DBApp();
        dbApp.init();

//        String clusteringKey = "55-6545";
//        Hashtable<String, Object> row = new Hashtable<>();
//
//        String table = "students";
//
//        row.put("name", "hamada");
//
//        dbApp.updateTable(table, clusteringKey, row);
//        dbApp = null;
    }

    @Test
    @Order(25)
    public void testUpdateCourses() throws Exception {
        DBApp dbApp = new DBApp();
        dbApp.init();

        String table = "courses";

        BufferedReader coursesTable = new BufferedReader(new FileReader("src/main/resources/courses_table.csv"));
        String record;
        Hashtable<String, Object> row = new Hashtable<>();
        int c = 1;

        String clusteringKey = "";
        while ((record = coursesTable.readLine()) != null && c > 0) {
            String[] fields = record.split(",");
            clusteringKey += fields[0].substring(0, 4);
            clusteringKey += "-";
            clusteringKey += fields[0].substring(5, 7);
            clusteringKey += "-";
            clusteringKey += fields[0].substring(8);

            c--;
        }

        coursesTable.close();

        row.put("course_id", "1100");
        row.put("course_name", "baaaar");
        row.put("hours", 13);


        dbApp.updateTable(table, clusteringKey, row);
        dbApp = null;
    }

    @Test
    @Order(26)
    public void testUpdateTranscripts() throws Exception {
        final DBApp dbApp = new DBApp();
        dbApp.init();

        String table = "transcripts";
        Hashtable<String, Object> row = new Hashtable();

        BufferedReader transcriptsTable = new BufferedReader(new FileReader("src/main/resources/transcripts_table.csv"));
        String record;

        int c = 1;
        String clusteringKey = "";
        while ((record = transcriptsTable.readLine()) != null && c > 0) {
            String[] fields = record.split(",");
            clusteringKey = fields[0].trim();

            c--;
        }

        transcriptsTable.close();

        row.put("student_id", "43-9874");
        row.put("course_name", "baaaar");

        String pattern = "yyyy-MM-dd";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
        Date date_passed = simpleDateFormat.parse("2011-04-01");
        row.put("date_passed", date_passed);

        dbApp.updateTable(table, clusteringKey, row);
    }

    @Test
    @Order(27)
    public void testUpdatePCs() throws Exception {
        final DBApp dbApp = new DBApp();
        dbApp.init();

        String table = "pcs";
        Hashtable<String, Object> row = new Hashtable();
        row.put("student_id", "51-3808");

        BufferedReader pcsTable = new BufferedReader(new FileReader("src/main/resources/pcs_table.csv"));
        String record;

        int c = 1;
        String clusteringKey = "";

        while ((record = pcsTable.readLine()) != null && c > 0) {
            String[] fields = record.split(",");
            clusteringKey = fields[0].trim();
            c--;

        }

        pcsTable.close();

        dbApp.updateTable(table, clusteringKey, row);

    }


    @Test
    @Order(28)
    public void testUpdateStudentsExtra() throws ParseException {
        final DBApp dbApp = new DBApp();
        dbApp.init();

        String table = "students";
        Hashtable<String, Object> row = new Hashtable();
        row.put("first_name", "foo");
        row.put("middle_name", "hamada");
        row.put("last_name", "bar");

        String pattern = "yyyy-MM-dd";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
        Date dob = simpleDateFormat.parse("1992-09-08");
        row.put("dob", dob);
        row.put("gpa", 1.1);

        Assertions.assertThrows(DBAppException.class, () -> {
            dbApp.updateTable(table, "82-8772", row);
        });

    }

    @Test
    @Order(29)
    public void testUpdateCoursesExtra() {
        final DBApp dbApp = new DBApp();
        dbApp.init();

        String table = "courses";
        Hashtable<String, Object> row = new Hashtable();
        row.put("elective", true);
        row.put("course_id", "foo");
        row.put("course_name", "bar");
        row.put("hours", 13);
        row.put("semester", 5);


        Assertions.assertThrows(DBAppException.class, () -> {
            dbApp.updateTable(table, "2000-04-01", row);
        });
    }

    @Test
    @Order(30)
    public void testUpdateTranscriptsExtra() throws ParseException {
        final DBApp dbApp = new DBApp();
        dbApp.init();

        String table = "transcripts";
        Hashtable<String, Object> row = new Hashtable();

        row.put("student_id", "34-9874");
        row.put("course_name", "bar");

        String pattern = "yyyy-MM-dd";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
        Date date_passed = simpleDateFormat.parse("2011-04-01");
        row.put("date_passed", date_passed);

        row.put("elective", true);

        Assertions.assertThrows(DBAppException.class, () -> {
            dbApp.updateTable(table, "1.57", row);
        });

    }

    @Test
    @Order(31)
    public void testUpdatePCsExtra() {
        final DBApp dbApp = new DBApp();
        dbApp.init();

        String table = "pcs";
        Hashtable<String, Object> row = new Hashtable();

        row.put("student_id", "79-0786");
        row.put("os", "linux");

        Assertions.assertThrows(DBAppException.class, () -> {

            dbApp.updateTable(table, "00353", row);

        });


    }


    @Test
    @Order(32)
    public void testStudentsDeletionComplex() throws Exception {
        final DBApp dbApp = new DBApp();
        dbApp.init();

//        String table = "students";
//        Hashtable<String, Object> row = new Hashtable();
//
//        BufferedReader studentsTable = new BufferedReader(new FileReader("src/main/resources/students_table.csv"));
//        String record;
//        int c = 0;
//        int finalLine = 1;
//
//        while ((record = studentsTable.readLine()) != null && c <= finalLine) {
//            if (c == finalLine) {
//                String[] fields = record.split(",");
//                row.put("id", fields[0]);
//
//                String pattern = "yyyy-MM-dd";
//                SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
//                Date dob = simpleDateFormat.parse(fields[5]);
//                row.put("dob", dob);
//
//                double gpa = Double.parseDouble(fields[2].trim());
//
//                row.put("gpa", gpa);
//
//            }
//            c++;
//        }
//        studentsTable.close();
//
//
//        dbApp.deleteFromTable(table, row);

    }

    @Test
    @Order(33)
    public void testCoursesDeleteComplex() throws Exception {
        final DBApp dbApp = new DBApp();
        dbApp.init();

        BufferedReader coursesTable = new BufferedReader(new FileReader("src/main/resources/courses_table.csv"));
        String record;
        Hashtable<String, Object> row = new Hashtable<>();
        int c = 0;
        int finalLine = 1;
        while ((record = coursesTable.readLine()) != null && c <= finalLine) {
            if (c == finalLine) {
                String[] fields = record.split(",");

                String pattern = "yyyy-MM-dd";
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
                Date dateAdded = simpleDateFormat.parse(fields[0]);
                row.put("date_added", dateAdded);
                row.put("course_name", fields[2]);


            }
            c++;
        }


        String table = "courses";

        dbApp.deleteFromTable(table, row);
    }

    @Test
    @Order(34)
    public void testTranscriptsDeleteComplex() throws Exception {
        final DBApp dbApp = new DBApp();
        dbApp.init();

        BufferedReader transcriptsTable = new BufferedReader(new FileReader("src/main/resources/transcripts_table.csv"));
        String record;
        Hashtable<String, Object> row = new Hashtable<>();
        int c = 0;
        int finalLimit = 1;
        while ((record = transcriptsTable.readLine()) != null && c <= finalLimit) {
            if (c == finalLimit) {
                String[] fields = record.split(",");
                row.put("gpa", Double.parseDouble(fields[0].trim()));
                row.put("course_name", fields[2].trim());
            }
            c++;
        }

        transcriptsTable.close();

        String table = "transcripts";

        dbApp.deleteFromTable(table, row);
    }

    @Test
    @Order(35)
    public void testPCsDeleteComplex() throws Exception {
        final DBApp dbApp = new DBApp();
        dbApp.init();

        BufferedReader pcsTable = new BufferedReader(new FileReader("src/main/resources/pcs_table.csv"));
        String record;
        Hashtable<String, Object> row = new Hashtable<>();
        int c = 0;
        int finalLine = 1;
        while ((record = pcsTable.readLine()) != null && c <= finalLine) {
            if(c == finalLine) {
                String[] fields = record.split(",");

                row.put("pc_id", Integer.parseInt(fields[0].trim()));
                row.put("student_id", fields[1].trim());
            }
            c++;
        }


        String table = "pcs";
        dbApp.deleteFromTable(table, row);
    }

    @Test
    @Order(36)
    public void testWrongStudentsKeyInsertion() throws ParseException {
        final DBApp dbApp = new DBApp();
        dbApp.init();

        String table = "students";
        Hashtable<String, Object> row = new Hashtable();
        row.put("id", 123);
        row.put("first_name", "foo");
        row.put("last_name", "bar");

        String pattern = "yyyy-MM-dd";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
        Date dob = simpleDateFormat.parse("1990-12-11");
        row.put("dob", dob);
        row.put("gpa", 1.1);

        Assertions.assertThrows(DBAppException.class, () -> {
                    dbApp.insertIntoTable(table, row);
                }
        );

    }

    @Test
    @Order(37)
    public void testWrongCoursesKeyInsertion() {
        final DBApp dbApp = new DBApp();
        dbApp.init();

        String table = "courses";
        Hashtable<String, Object> row = new Hashtable();
        row.put("course_id", "foo");
        row.put("course_name", "bar");
        row.put("hours", 13);


        row.put("date_added", "1990-12-11");


        Assertions.assertThrows(DBAppException.class, () -> {
                    dbApp.insertIntoTable(table, row);
                }
        );

    }

    @Test
    @Order(38)
    public void testWrongTranscriptsKeyInsertion() throws ParseException, ParseException {
        final DBApp dbApp = new DBApp();
        dbApp.init();

        String table = "transcripts";
        Hashtable<String, Object> row = new Hashtable();
        row.put("gpa","string");
        row.put("student_id", "foo");
        row.put("course_name", "bar");


        String pattern = "yyyy-MM-dd";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
        Date date_passed = simpleDateFormat.parse("1990-12-11");
        row.put("date_passed", date_passed);


        Assertions.assertThrows(DBAppException.class, () -> {
                    dbApp.insertIntoTable(table, row);
                }
        );

    }

    @Test
    @Order(39)
    public void testWrongPCsKeyInsertion() {
        final DBApp dbApp = new DBApp();
        dbApp.init();

        String table = "pcs";
        Hashtable<String, Object> row = new Hashtable();
        row.put("pc_id", "true");
        row.put("student_id", "foo");

        Assertions.assertThrows(DBAppException.class, () -> {
                    dbApp.insertIntoTable(table, row);
                }
        );
    }

    @Test
    @Order(40)
    public void testSelectFromMultipleTables() throws DBAppException, IOException, ParseException, ClassNotFoundException {
        final DBApp dbApp = new DBApp();
        dbApp.init();

        SQLTerm[] arrSQLTerms;
        arrSQLTerms = new SQLTerm[2];
        arrSQLTerms[0] = new SQLTerm();
        arrSQLTerms[0]._strTableName = "students";
        arrSQLTerms[0]._strColumnName= "name";
        arrSQLTerms[0]._strOperator = "=";
        arrSQLTerms[0]._objValue = "ahmed";

        arrSQLTerms[1] = new SQLTerm();
        arrSQLTerms[1]._strTableName = "transcripts";
        arrSQLTerms[1]._strColumnName= "gpa";
        arrSQLTerms[1]._strOperator = "<=";
        arrSQLTerms[1]._objValue = 3.8925;

        String[]strarrOperators = new String[1];
        strarrOperators[0] = "OR";

        Assertions.assertThrows(DBAppException.class, () -> dbApp.selectFromTable(arrSQLTerms, strarrOperators));

    }

    @Test
    @AfterAll
    public static void testDeleteAllTuplesFromStudent() throws DBAppException, IOException, ParseException, ClassNotFoundException {
        final DBApp dbApp = new DBApp();
        dbApp.init();

        dbApp.printIndex("students", new String[]{"age", "name", "gpa"});
        dbApp.printTable("students");

        String table = "students";
        Hashtable<String, Object> row = new Hashtable();
        dbApp.deleteFromTable(table, row);
        File pages = new File(dataDirPath + "students/Pages");
        Assertions.assertEquals(0, pages.listFiles().length);
    }

    private void createStudentTable(DBApp dbApp) throws Exception {
        // String CK
        String tableName = "students";

        Hashtable htblColNameType = new Hashtable();
        htblColNameType.put("id", "java.lang.String");
        htblColNameType.put("name", "java.lang.String");
        htblColNameType.put("gpa", "java.lang.double");
        htblColNameType.put("age", "java.lang.Integer");
        htblColNameType.put("salary", "java.lang.double");
        htblColNameType.put("dob", "java.util.Date");

        Hashtable<String, String> htblColNameMin = new Hashtable<>();
        htblColNameMin.put("id", "40-0000");
        htblColNameMin.put("name", "aaaaaaaaaaa");
        htblColNameMin.put("gpa", "0.7");
        htblColNameMin.put("age", "0");
        htblColNameMin.put("salary", "1000.0");
        htblColNameMin.put("dob", "1900-01-01");

        Hashtable<String, String> htblColNameMax = new Hashtable<>();
        htblColNameMax.put("id", "61-9999");
        htblColNameMax.put("name", "zzzzzzzzzzz");
        htblColNameMax.put("gpa", "4.0");
        htblColNameMax.put("age", "100");
        htblColNameMax.put("salary", "10000000.0");
        htblColNameMax.put("dob", "2023-05-12");

        dbApp.createTable(tableName, "id", htblColNameType, htblColNameMin, htblColNameMax);
    }


    private void createCoursesTable(DBApp dbApp) throws Exception {
        // Date CK
        String tableName = "courses";

        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
        htblColNameType.put("date_added", "java.util.Date");
        htblColNameType.put("course_id", "java.lang.String");
        htblColNameType.put("course_name", "java.lang.String");
        htblColNameType.put("hours", "java.lang.Integer");


        Hashtable<String, String> minValues = new Hashtable<>();
        minValues.put("date_added", "1901-01-01");
        minValues.put("course_id", "0000");
        minValues.put("course_name", "AAAAAA");
        minValues.put("hours", "1");

        Hashtable<String, String> maxValues = new Hashtable<>();
        maxValues.put("date_added", "2020-12-31");
        maxValues.put("course_id", "9999");
        maxValues.put("course_name", "zzzzzz");
        maxValues.put("hours", "24");

        dbApp.createTable(tableName, "date_added", htblColNameType, minValues, maxValues);

    }

    private void createTranscriptsTable(DBApp dbApp) throws Exception {
        // Double CK
        String tableName = "transcripts";

        Hashtable<String, String> htblColNameType = new Hashtable<>();
        htblColNameType.put("gpa", "java.lang.Double");
        htblColNameType.put("student_id", "java.lang.String");
        htblColNameType.put("course_name", "java.lang.String");
        htblColNameType.put("date_passed", "java.util.Date");

        Hashtable<String, String> minValues = new Hashtable<>();
        minValues.put("gpa", "0.7");
        minValues.put("student_id", "43-0000");
        minValues.put("course_name", "AAAAAA");
        minValues.put("date_passed", "1990-01-01");

        Hashtable<String, String> maxValues = new Hashtable<>();
        maxValues.put("gpa", "5.0");
        maxValues.put("student_id", "99-9999");
        maxValues.put("course_name", "zzzzzz");
        maxValues.put("date_passed", "2020-12-31");

        dbApp.createTable(tableName, "gpa", htblColNameType, minValues, maxValues);
    }


    private void createPCsTable(DBApp dbApp) throws Exception {
        // Integer CK
        String tableName = "pcs";

        Hashtable<String, String> htblColNameType = new Hashtable<>();
        htblColNameType.put("pc_id", "java.lang.Integer");
        htblColNameType.put("student_id", "java.lang.String");


        Hashtable<String, String> minValues = new Hashtable<>();
        minValues.put("pc_id", "0");
        minValues.put("student_id", "43-0000");

        Hashtable<String, String> maxValues = new Hashtable<>();
        maxValues.put("pc_id", "20000");
        maxValues.put("student_id", "99-9999");

        dbApp.createTable(tableName, "pc_id", htblColNameType, minValues, maxValues);
    }

    private void insertStudentRecords(DBApp dbApp, int limit) throws Exception {
        BufferedReader studentsTable = new BufferedReader(new FileReader("src/main/resources/students_table.csv"));
        String record;
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
                row.put("age", Integer.parseInt(fields[3]));
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
                row.put("age", Integer.parseInt(fields[3]));
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
                row.put("age", Integer.parseInt(fields[3]));
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
            row.put("age", Integer.parseInt(fields[3]));
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
    }

    private void insertPCsRecords(DBApp dbApp, int limit) throws Exception {
        BufferedReader pcsTable = new BufferedReader(new FileReader("src/main/resources/pcs_table.csv"));
        String record;
        Hashtable<String, Object> row = new Hashtable<>();
        int c = limit;
        if (limit == -1) {
            c = 1;
        }
        while ((record = pcsTable.readLine()) != null && c > 0) {
            String[] fields = record.split(",");

            row.put("pc_id", Integer.parseInt(fields[0].trim()));
            row.put("student_id", fields[1].trim());

            dbApp.insertIntoTable("pcs", row);
            row.clear();

            if (limit != -1) {
                c--;
            }
        }

        pcsTable.close();
    }

    private void insertTranscriptsRecords(DBApp dbApp, int limit) throws Exception {
        BufferedReader transcriptsTable = new BufferedReader(new FileReader("src/main/resources/transcripts_table.csv"));
        String record;
        Hashtable<String, Object> row = new Hashtable<>();
        int c = limit;
        if (limit == -1) {
            c = 1;
        }
        while ((record = transcriptsTable.readLine()) != null && c > 0) {
            String[] fields = record.split(",");

            row.put("gpa", Double.parseDouble(fields[0].trim()));
            row.put("student_id", fields[1].trim());
            row.put("course_name", fields[2].trim());

            String pattern = "yyyy-MM-dd";
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
            Date dateUsed = simpleDateFormat.parse(fields[3]);
            row.put("date_passed", dateUsed);

            dbApp.insertIntoTable("transcripts", row);
            row.clear();

            if (limit != -1) {
                c--;
            }
        }

        transcriptsTable.close();
    }

    private void insertCoursesRecords(DBApp dbApp, int limit) throws Exception {
        BufferedReader coursesTable = new BufferedReader(new FileReader("src/main/resources/courses_table.csv"));
        String record;
        Hashtable<String, Object> row = new Hashtable<>();
        int c = limit;
        if (limit == -1) {
            c = 1;
        }
        while ((record = coursesTable.readLine()) != null && c > 0) {
            String[] fields = record.split(",");

            String pattern = "yyyy-MM-dd";
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
            Date dateAdded = simpleDateFormat.parse(fields[0]);

            row.put("date_added", dateAdded);

            row.put("course_id", fields[1]);
            row.put("course_name", fields[2]);
            row.put("hours", Integer.parseInt(fields[3]));

            dbApp.insertIntoTable("courses", row);
            row.clear();

            if (limit != -1) {
                c--;
            }
        }

        coursesTable.close();
    }
}