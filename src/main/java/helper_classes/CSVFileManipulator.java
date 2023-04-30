package helper_classes;
import exceptions.DBAppException;

import java.text.*;
import java.util.*;
import java.io.*;
public class CSVFileManipulator {

    public static void validate(String colType, String colMin, String colMax) throws DBAppException {
        boolean isValid = true;
        switch (colType) {
            case "java.lang.Integer" :  try {
                                            if (Integer.parseInt(colMin) > Integer.parseInt(colMax))
                                                isValid &= false;
                                        } catch (Exception e) {
                                            isValid &= false;
                                        }
                                            break;
            case "java.lang.String" :   if (colMin.compareTo(colMax) > 0)
                                            isValid &= false;
                                        break;
            case "java.lang.Double" :
            case "java.lang.double" :   try {
                                            if (Double.parseDouble(colMin) > Double.parseDouble(colMax))
                                                isValid &= false;
                                        } catch (Exception e) {
                                            isValid &= false;
                                        }
                                        break;
            case "java.util.Date" :     try {
                                            String pattern = "yyyy-MM-dd";
                                            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
                                            Date dateMin = simpleDateFormat.parse(colMin);
                                            Date dateMax = simpleDateFormat.parse(colMax);
                                            if (dateMin.compareTo(dateMax) > 0)
                                                isValid &= false;
                                        } catch (Exception e) {

                                            isValid &= false;
                                        }
                                        break;
            default: isValid &= false;
        }

        if(!isValid)
            throw new DBAppException("Invalid min/max values for column type");
    }
    public static void write(String strTableName,
                             Hashtable<String, String> htblColNameType,
                             Hashtable<String, String> htblColNameMin,
                             Hashtable<String, String> htblColNameMax,
                             Vector<String> colNames) throws IOException, DBAppException {
        String fileName = "src/main/resources/metadata.csv";
        FileWriter csvWriter = new FileWriter(fileName, true);
        for(int i=0 ; i<colNames.size() ; i++){
            String colName = colNames.get(i);
            String colType = htblColNameType.get(colName);
            String colMin = htblColNameMin.get(colName);
            String colMax = htblColNameMax.get(colName);
            validate(colType, colMin, colMax);
            String[] data = {strTableName, colName, colType, "false", "null", "null", colMin, colMax};
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
    ) throws IOException, ParseException {
        String fileName = "src/main/resources/metadata.csv";
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
            lineScanner.next(); //index name
            lineScanner.next(); //index type
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
                case "java.util.Date" :     String pattern = "yyyy-MM-dd";
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
                    Date dateMin = simpleDateFormat.parse(htblColNameMin.get(colName));
                    Date dateMax = simpleDateFormat.parse(htblColNameMax.get(colName));
                    colMin.add(dateMin);
                    colMax.add(dateMax);
                    break;
                default: break;
            }
        }
    }



}
