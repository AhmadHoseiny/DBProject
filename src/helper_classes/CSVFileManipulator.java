package helper_classes;
import exceptions.DBAppException;

import java.util.*;
import java.io.*;
public class CSVFileManipulator {

    public static void validate(String colType, String colMin, String colMax) throws DBAppException {
        boolean isValid = true;
        switch (colType) {
            case "java.lang.Integer" :  try {
                                            Integer.parseInt(colMin);
                                            Integer.parseInt(colMax);
                                        } catch (Exception e) {
                                            isValid &= false;
                                        }
                                        break;
            case "java.lang.String" :  break;
            case "java.lang.Double" :
            case "java.lang.double" :   try {
                                            Double.parseDouble(colMin);
                                            Double.parseDouble(colMax);
                                        } catch (Exception e) {
                                            isValid &= false;
                                        }
                                        break;
            case "java.util.Date" :     try {
                                            Date.parse(colMin);
                                            Date.parse(colMax);
                                        } catch (Exception e) {
                                            isValid &= false;
                                        }
                                        break;
            default: isValid &= false;
        }
        isValid &= colMin.compareTo(colMax) <= 0; // min <= max
        if(!isValid)
            throw new DBAppException("Invalid min/max values for column type");
    }
    public static void write(String strTableName,
                             Hashtable<String, String> htblColNameType,
                             Hashtable<String, String> htblColNameMin,
                             Hashtable<String, String> htblColNameMax,
                             Vector<String> colNames) throws IOException, DBAppException {
        String fileName = "src/metadata.csv";
        FileWriter csvWriter = new FileWriter(fileName, true);
        for(int i=0 ; i<colNames.size() ; i++){
            String colName = colNames.get(i);
            String colType = htblColNameType.get(colName);
            String colMin = htblColNameMin.get(colName);
            String colMax = htblColNameMax.get(colName);
            validate(colType, colMin, colMax);
            String[] data = {strTableName, colName, colType, "false", "false", colMin, colMax};
            // this is the clustering key
            if(i==0)
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
                            Vector<Comparable> colMax
                            ) throws IOException {
        String fileName = "src/metadata.csv";
        File file = new File(fileName);
        Scanner fileScanner = new Scanner(file);
        HashMap<String, String> htblColNameType = new HashMap<>();
        HashMap<String, String> htblColNameMin = new HashMap<>();
        HashMap<String, String> htblColNameMax = new HashMap<>();
        while (fileScanner.hasNextLine()) {
            String line = fileScanner.nextLine();
            Scanner lineScanner = new Scanner(line);
            lineScanner.useDelimiter(",");
            String tableName = lineScanner.next();
            if(!tableName.equals(strTableName))
                continue;
            String colName = lineScanner.next();
            htblColNameType.put(colName, lineScanner.next());
            lineScanner.next(); //clustered key
            lineScanner.next(); //indexed key
            htblColNameMin.put(colName, lineScanner.next());
            htblColNameMax.put(colName, lineScanner.next());
            lineScanner.close();
        }
        fileScanner.close();
        for(int i=0 ; i<colNames.size() ; i++){
            String colName = colNames.get(i);
            colTypes.add(htblColNameType.get(colName));
            switch (htblColNameType.get(colName)) {
                case "java.lang.Integer" :  colMin.add(Integer.parseInt(htblColNameMin.get(colName)));
                                            colMax.add(Integer.parseInt(htblColNameMax.get(colName)));
                                            break;
                case "java.lang.String" :   colMin.add(htblColNameMin.get(colName));
                                            colMax.add(htblColNameMax.get(colName));
                                            break;
                case "java.lang.Double" :
                case "java.lang.double" :   colMin.add(Double.parseDouble(htblColNameMin.get(colName)));
                                            colMax.add(Double.parseDouble(htblColNameMax.get(colName)));
                                            break;
                case "java.util.Date" :     colMin.add(Date.parse(htblColNameMin.get(colName)));
                                            colMax.add(Date.parse(htblColNameMax.get(colName)));
                                            break;
                default: break;
            }
        }
    }

}
