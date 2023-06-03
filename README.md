# Database Management System

- [Navigation](#navigation)
    - [Introduction](#introduction)
    - [Functionalities](#functionalities)
    - [Installation](#installation)
    - [Usage](#usage)
    - [Tools](#tools)
    - [Contributors](#contributors)

## Introduction

This is a simple database management system that allows users to create, read, update, and delete data from a database. The database is stored in a file called `Data` and is created if it does not exist. The database is a simple key-value(s) store that stores data in a Vector format. The database is implemented in java, and the data is serialized to the disk and deserialized whenever needed. To optimize queries and other functionalities, binary search is used in insert, update, single delete, and point query functionalities.

## Functionalities

The database management system allows users to perform the following operations:

- Create a table

- Insert data into a table

- Read data from a table

- Update data in a table

- Delete data from a table

- Create an Octree index on a table

- Perform a range query on a table

- Perform a point query on a table

- Perform a compound query on a table

Each functionality from the above list can be used in SQL queries. The following is the syntax for each of the functionalities:

- Create a table

  ```sql
  CREATE TABLE table_name (column_name1 data_type, column_name2 data_type, ...);
  ```

- Insert data into a table

  ```sql
    INSERT INTO table_name VALUES (value1, value2, ...);
    ```

- Read data from a table

    ```sql
    SELECT * FROM table_name;
    ```

- Update data in a table

    ```sql
    UPDATE table_name SET column_name1 = value1, column_name2 = value2, ... WHERE column_name = value;
    ```

- Delete data from a table

    ```sql
    DELETE FROM table_name WHERE column_name = value;
    ```

- Create an Octree index on a table

    ```sql
    CREATE INDEX ON table_name (column_name);
    ```
<details>

 <summary> project structure </summary>
- Perform a range query on a table

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

- Perform a point query on a table

    ```sql
    SELECT * FROM table_name WHERE column_name = value;
    ```

- Perform a compound query on a table

    ```sql
    SELECT * FROM table_name WHERE column_name1 = value1 AND column_name2 = value2;
    ```

    ```sql
    SELECT * FROM table_name WHERE column_name1 = value1 OR column_name2 = value2;
    ```

    ```sql
    SELECT * FROM table_name WHERE column_name1 = value1 AND column_name2 = value2 OR column_name3 = value3;
    ```

## Installation

To install the database management system, clone the repository using the following command:

```bash
git clone https://github.com/AhmadHoseiny/DBProject.git
```

## Usage

To use the database management system, clone the repo, compile the code, and run the `DBApp` class. The following is an example of how to use the database management system:

```java
DBApp dbApp = new DBApp();
```
Next you will have to create parameters that constraint the input you introduce to each field in the table.
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
Then you will have to create a table with name `students`, primary key `id`, and the parameters you created above:
```java
dbApp.createTable("students", "id", htblColNameType, minValues, maxValues);
```
To insert data into the table, you will have to create a `Hashtable` with the column names as keys and the values as values:
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
- Note that: if you want to insert a null value you can do it by not entering the column name in the `Hashtable`:
    ```java
    htblColNameValue = new Hashtable<String, Object>();
    htblColNameValue.put("id", 4);
    htblColNameValue.put("name", "Logine");

    dbApp.insertIntoTable("students", htblColNameValue);
    ```
To update data in the table, you will have to create a `Hashtable` with the column names as keys and the values as values:
```java
Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
htblColNameValue.put("name", "Ahmad");
htblColNameValue.put("gpa", 3.5);

dbApp.updateTable("students", "1", htblColNameValue);
```
To delete data from the table, you will have to create a `Hashtable` with the column names as keys and the values as values:
```java
Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
htblColNameValue.put("id", 1);

dbApp.deleteFromTable("students", htblColNameValue);
```
- Note that: if you want to delete all the data in the table, you can do it by not entering any column name in the `Hashtable`:
    ```java
    htblColNameValue = new Hashtable<String, Object>();

    dbApp.deleteFromTable("students", htblColNameValue);
    ```
To create an Octree index on a table, you will have to create an `Array` with the column names as values:
```java
String[] strarrColNames = { "id", "name", "gpa" };

dbApp.createIndex("students", strarrColNames);
```
To perform a point query on a table, you will have to create a `Hashtable` with the column names as keys and the values as values:
- Note that: the returned result set has to be stored in a MyIterator object. This way you can iterate over the result set and print the data.
```java
Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
htblColNameValue.put("id", 1);

MyIterator resultSet = dbApp.selectFromTable("students", htblColNameValue);

while (resultSet.hasNext()) {
    System.out.println(resultSet.next());
}
```
Performing a range query is supported in SQL format. The following is an example of a range query:
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
To print any table you can use the following method:
```java
dbApp.printTable("students");
```
To print any octree index you can use the following method:
```java
String[] strarrColNames = { "id", "name", "gpa" };

dbApp.printIndex("students", strarrColNames);
```

## Tools

- [Java](https://www.java.com/en/)
- [IntelliJ IDEA](https://www.jetbrains.com/idea/)
- [Git](https://git-scm.com/)
- [Maven](https://maven.apache.org/)
- [Antlr](https://www.antlr.org/)

## Contributors

#### This project has been implemented by a team of five computer engineering students

- [Ahmad Hoseiny](https://github.com/AhmadHoseiny)
- [Omar Wael](https://github.com/o-wael)
- [Abdelrahman Salah](https://github.com/19AbdelrahmanSalah19)
- [Ali Hussein](https://github.com/AliAdam102002)
- [Logine Mohamed](https://github.com/logine20)
