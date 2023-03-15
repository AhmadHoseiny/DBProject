import java.io.*;
import java.util.*;

import exceptions.DBAppException;
import helper_classes.*;

public class Table implements Serializable {
    String tableName;
    String clusteringKey;
    Vector<String> colNames;
    Vector<String> colTypes;
    Vector<String> colMin;
    Vector<String> colMax;
    Vector<Page> table;

    public Table(String strTableName,
                 String strClusteringKeyColumn,
                 Hashtable<String, String> htblColNameType,
                 Hashtable<String, String> htblColNameMin,
                 Hashtable<String, String> htblColNameMax) throws DBAppException {
        if(htblColNameType.size() != htblColNameMin.size() ||
                htblColNameMin.size() !=  htblColNameMax.size() ||
                htblColNameType.size() != htblColNameMax.size())
            throw new DBAppException("The input data is inconsistent");
        this.tableName = strTableName;
        this.clusteringKey = strClusteringKeyColumn;
        this.colNames = new Vector<>();
        this.colTypes = new Vector<>();
        this.colMin = new Vector<>();
        this.colMax = new Vector<>();
        for(Map.Entry<String, String> e : htblColNameType.entrySet()){
            colNames.add(e.getKey());
            colTypes.add(e.getValue());
        }
        for(String colName : colNames) {
            colMin.add(htblColNameMin.get(colName));
            colMax.add(htblColNameMax.get(colName));
        }
        this.table = new Vector<>();
    }
}
