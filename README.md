# Database Management System

- [Navigation](#navigation)
  - [Introduction](#introduction)
  - [Functionalities](#functionalities)
  - [Installation](#installation)
  - [Usage](#usage)
  - [Tools](#tools)
  - [Contributors](#contributors)

<br>

## Introduction

This is a simple database management system that allows users to create, read, update, and delete data from a database. The database is stored in a file called <span style="font-weight: 700">Data</span> and is created if it does not exist. The database is a simple key-value(s) store that stores data in a Vector format. The database is implemented in java, and the data is serialized to the disk and deserialized whenever needed. To optimize queries and other functionalities, binary search is used in insert, update, single delete, and point query functionalities.

<br>

## Functionalities

The database management system allows users to perform the following operations:


<details>
     <summary>Create a table</summary>

```sql
CREATE TABLE table_name (column_name1 data_type, column_name2 data_type, ...);
```

</details>

<br>

<details>
     <summary>Insert data into a table</summary>

```sql
INSERT INTO table_name VALUES (value1, value2, ...);
```
</details>

<br>

<details>
     <summary>Read data from a table</summary>

```sql
SELECT * FROM table_name;
```

</details>

<br>

<details>
     <summary>Update data in a table</summary>

```sql
UPDATE table_name SET column_name1 = value1, column_name2 = value2, ... WHERE column_name = value;
```

</details>

<br>

<details>
     <summary>Delete data from a table</summary>

```sql
DELETE FROM table_name WHERE column_name = value;
```

</details>

<br>

<details>
     <summary>Create an Octree index on a table</summary>

```sql
CREATE INDEX ON table_name (column_name);
```

</details>

<br>

<details>

 <summary>Perform a range query on a table </summary>

```sql
SELECT * FROM table_name WHERE column_name < value1;
```

```sql
SELECT * FROM table_name WHERE column_name > value1;
```

```sql
SELECT * FROM table_name WHERE column_name != value1;   
```

```sql
SELECT * FROM table_name WHERE column_name <> value1;
```

</details>

<br>

<details>

 <summary> Perform a point query on a table </summary>

```sql
SELECT * FROM table_name WHERE column_name = value;
```

</details>

<br>

<details>
    <summary>Perform a compound query on a table</summary>
<!-- - Perform a compound query on a table -->

```sql
SELECT * FROM table_name WHERE column_name1 = value1 AND column_name2 = value2;
```

```sql
SELECT * FROM table_name WHERE column_name1 = value1 OR column_name2 = value2;
```

```sql
SELECT * FROM table_name WHERE column_name1 = value1 AND column_name2 = value2 OR column_name3 = value3;
```

</details>

<br>

<br>

## Installation

To install the database management system, clone the repository using the following command:

```bash
git clone https://github.com/AhmadHoseiny/DBProject.git
```

<br>

## Usage

To use the database management system, clone the repo, compile the code, and run the <span style="font-weight: 700">DBApp</span> class. The following is an example of how to use the database management system:

```java
DBApp dbApp = new DBApp();
```

<details>
    <summary> Next you will have to create parameters that constraint the input you introduce to each field in the table. </summary>

```java
Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
htblColNameType.put("id", "java.lang.Integer");
htblColNameType.put("name", "java.lang.String");
htblColNameType.put("gpa", "java.lang.double");

Hashtable<String, Object> minValues = new Hashtable<>();
minValues.put("id", 0);
minValues.put("name", "A");
minValues.put("gpa", 0.0);

Hashtable<String, Object> maxValues = new Hashtable<>();
maxValues.put("id", 1000);
maxValues.put("name", "ZZ");
maxValues.put("gpa", 4.0);
```

</details>

<br>

<details>
    <summary> Then you will have to create a table with name <span style="font-weight: 700">students</span>, primary key <span style="font-weight: 700">id</span>, and the parameters you created above </summary>


```java
dbApp.createTable("students", "id", htblColNameType, minValues, maxValues);
```
</details>

<br>

<details>
    <summary> To insert data into the table, you will have to create a <span style="font-weight: 700">Hashtable</span> with the column names as keys and the values as values </summary>

```java
Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
htblColNameValue.put("id", 1);
htblColNameValue.put("name", "Ahmad");
htblColNameValue.put("gpa", 3.5);

htblColNameValue = new Hashtable<String, Object>();
htblColNameValue.put("id", 2);
htblColNameValue.put("name", "Omar");
htblColNameValue.put("gpa", 3.0);

htblColNameValue = new Hashtable<String, Object>();
htblColNameValue.put("id", 3);
htblColNameValue.put("name", "Abdelrahman");
htblColNameValue.put("gpa", 3.8);

dbApp.insertIntoTable("students", htblColNameValue);
dbApp.insertIntoTable("students", htblColNameValue);
dbApp.insertIntoTable("students", htblColNameValue);
```

- Note that: if you want to insert a null value you can do it by not entering the column name in the <span style="font-weight: 700">Hashtable</span>, null values are wrapped in a <span style="font-weight: 700">NullWrapper</span> class instance.

    ```java
    htblColNameValue = new Hashtable<String, Object>();
    htblColNameValue.put("id", 4);
    htblColNameValue.put("name", "Logine");

    dbApp.insertIntoTable("students", htblColNameValue);
    ```

</details>

<br>

<details>

<summary> To update data in the table, you will have to create a <span style="font-weight: 700">Hashtable</span> with the column names as keys and the values as values </summary>

```java
Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
htblColNameValue.put("name", "Ahmad");
htblColNameValue.put("gpa", 3.5);

dbApp.updateTable("students", "1", htblColNameValue);
```

</details>

<br>

<details>

<summary> To delete data from the table, you will have to create a <span style="font-weight: 700">Hashtable</span> with the column names as keys and the values as values </summary>

```java
Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
htblColNameValue.put("id", 1);

dbApp.deleteFromTable("students", htblColNameValue);
```
- Note that: if you want to delete all the data in the table, you can do it by not entering any column name in the <span style="font-weight: 700">Hashtable</span>

    ```java
    htblColNameValue = new Hashtable<String, Object>();

    dbApp.deleteFromTable("students", htblColNameValue);
    ```

</details>

<br>

<details>

<summary> To create an Octree index on a table, you will have to create an <span style="font-weight: 700">Array</span> with the column names as values </summary>

```java
String[] strarrColNames = { "id", "name", "gpa" };

dbApp.createIndex("students", strarrColNames);
```

</details>

<br>

<details>

<summary> To perform a point query on a table, you will have to create a <span style="font-weight: 700">Hashtable</span> with the column names as keys and the values as values </summary>

- Note that: the returned result set has to be stored in a <span style="font-weight: 700">MyIterator</span> object. This way you can iterate over the result set and print the data.
```java
Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
htblColNameValue.put("id", 1);

MyIterator resultSet = dbApp.selectFromTable("students", htblColNameValue);

while (resultSet.hasNext()) {
    System.out.println(resultSet.next());
}
```

</details>

<br>

<details>

<summary> Performing a range query is supported in SQL format. The following is an example of a range query </summary>

```java
StringBuffer strbufSQL = new StringBuffer();
strbufSQL.append("SELECT * FROM students WHERE id < 2;");
MyIterator resultSet = dbApp.parseSQL(strbufSQL);

while (resultSet.hasNext()) {
    System.out.println(resultSet.next());
}
```
- Note that: creating a table, inserting into, updating, and deleting from a table are also supported in SQL format. The following is an example of how to use the database management system in SQL format:
    ```java
    StringBuffer strbufSQL = new StringBuffer();
    strbufSQL.append(any SQL query here);

    dbApp.parseSQL(strbufSQL);
    ```
</details>

<br>

<details>

<summary> To print any table you can use the following method </summary>

```java
dbApp.printTable("students");
```

</details>

<br>

<details>

<summary> To print any octree index you can use the following method </summary>

```java
String[] strarrColNames = { "id", "name", "gpa" };

dbApp.printIndex("students", strarrColNames);
```

</details>

<br>

## Tools

- [Java](https://www.java.com/en/)
- [IntelliJ IDEA](https://www.jetbrains.com/idea/)
- [Git](https://git-scm.com/)
- [Maven](https://maven.apache.org/)
- [Antlr](https://www.antlr.org/)

<br>

## Contributors

#### This project has been implemented by a team of five computer engineering students

- [Ahmad Hoseiny](https://github.com/AhmadHoseiny)
- [Omar Wael](https://github.com/o-wael)
- [Abdelrahman Salah](https://github.com/19AbdelrahmanSalah19)
- [Ali Hussein](https://github.com/AliAdam102002)
- [Logine Mohamed](https://github.com/logine20)
