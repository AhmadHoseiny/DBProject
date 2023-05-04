package helper_classes;

import exceptions.DBAppException;
import tables.Table;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class CSVFileManipulator {


    public static void write(String strTableName,
                             Hashtable<String, String> htblColNameType,
                             Hashtable<String, String> htblColNameMin,
                             Hashtable<String, String> htblColNameMax,
                             Vector<String> colNames) throws IOException, DBAppException {
        String fileName = "src/main/resources/metadata.csv";
        FileWriter csvWriter = new FileWriter(fileName, true);
        for (int i = 0; i < colNames.size(); i++) {
            String colName = colNames.get(i);
            String colType = htblColNameType.get(colName);
            String colMin = htblColNameMin.get(colName);
            String colMax = htblColNameMax.get(colName);
            MetadataValidator.validate(colType, colMin, colMax);
            String[] data = {strTableName, colName, colType, "false", "null", "null", colMin, colMax};
            // this is the clustering key
            if (i == 0)
                data[3] = "true";
            csvWriter.append(String.join(",", data));
            csvWriter.append("\n");
        }
        csvWriter.flush();
        csvWriter.close();
    }


    public static void read(String strTableName,
                            Vector<String> colNames,
                            Vector<String> colTypes,
                            Vector<Comparable> colMin,
                            Vector<Comparable> colMax,
                            Vector<String> indexNames
    ) throws IOException, ParseException {
        String fileName = "src/main/resources/metadata.csv";
        File file = new File(fileName);
        Scanner fileScanner = new Scanner(file);
        HashMap<String, String> htblColNameType = new HashMap<>();
        HashMap<String, String> htblColNameMin = new HashMap<>();
        HashMap<String, String> htblColNameMax = new HashMap<>();
        HashMap<String, String> htblColNameIndexName = new HashMap<>();
        while (fileScanner.hasNextLine()) {
            String line = fileScanner.nextLine();
            Scanner lineScanner = new Scanner(line);
            lineScanner.useDelimiter(",");
            String tableName = lineScanner.next();
            if (!tableName.equals(strTableName))
                continue;
            String colName = lineScanner.next();
            htblColNameType.put(colName, lineScanner.next());
            lineScanner.next(); //clustered key
            htblColNameIndexName.put(colName, lineScanner.next()); //index name
            lineScanner.next(); //index type
            htblColNameMin.put(colName, lineScanner.next());
            htblColNameMax.put(colName, lineScanner.next());
            lineScanner.close();
        }
        fileScanner.close();
        for (int i = 0; i < colNames.size(); i++) {
            String colName = colNames.get(i);
            colTypes.add(htblColNameType.get(colName));
            switch (htblColNameType.get(colName)) {
                case "java.lang.Integer":
                    colMin.add(Integer.parseInt(htblColNameMin.get(colName)));
                    colMax.add(Integer.parseInt(htblColNameMax.get(colName)));
                    break;
                case "java.lang.String":
                    colMin.add(htblColNameMin.get(colName));
                    colMax.add(htblColNameMax.get(colName));
                    break;
                case "java.lang.Double":
                case "java.lang.double":
                    colMin.add(Double.parseDouble(htblColNameMin.get(colName)));
                    colMax.add(Double.parseDouble(htblColNameMax.get(colName)));
                    break;
                case "java.util.Date":
                    String pattern = "yyyy-MM-dd";
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
                    Date dateMin = simpleDateFormat.parse(htblColNameMin.get(colName));
                    Date dateMax = simpleDateFormat.parse(htblColNameMax.get(colName));
                    colMin.add(dateMin);
                    colMax.add(dateMax);
                    break;
                default:
                    break;
            }
            String indexName = htblColNameIndexName.get(colName);
            indexNames.add(indexName.equals("null") ? null : indexName);
        }
    }


    //assume strarrColName is sorted
    //When an index is created, the metadata is updated to reflect that
    public static void updateUponIndexCreation(Table t, String[] strarrColName) throws IOException, ParseException {
        String strTableName = t.getTableName();
        String indexName = IndexNameGetter.getIndexName(strarrColName);
        HashSet<String> hs = new HashSet<>();
        for (String x : strarrColName) {
            hs.add(x);
        }

        String oldFileName = "src/main/resources/metadata.csv";
        File oldFile = new File(oldFileName);
        Scanner fileScanner = new Scanner(oldFile);

        String newFileName = "src/main/resources/tempMetadata.csv";
        File newFile = new File(newFileName);
        FileWriter csvWriter = new FileWriter(newFile, true);

        while (fileScanner.hasNextLine()) {
            String line = fileScanner.nextLine();
            Scanner lineScanner = new Scanner(line);
            lineScanner.useDelimiter(",");

            String data[] = new String[8];
            for (int i = 0; i < 8; i++) {
                data[i] = lineScanner.next();
            }
            lineScanner.close();

            if (data[0].equals(strTableName) && hs.contains(data[1])) {
                data[4] = indexName;
                data[5] = "Octree";
            }

            csvWriter.append(String.join(",", data));
            csvWriter.append("\n");
        }
        fileScanner.close();

        csvWriter.flush();
        csvWriter.close();

        oldFile.delete();
        newFile.renameTo(new File(oldFileName));

        t.initializeTable();
    }


}
