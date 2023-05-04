package helper_classes;

import tables.*;
import index.*;

import java.io.*;
import java.util.*;
public class AllRecordInIndexInserter {

    static final String directoryPathResourcesData = "src/main/resources/Data/";
    Table t;
    Octree ot;

    public AllRecordInIndexInserter(Table t, Octree ot) {
        this.t = t;
        this.ot = ot;
    }
    public void insertAllRecords() throws IOException, ClassNotFoundException {

        File folder = new File(directoryPathResourcesData + t.getTableName()+ "/Pages");
        int fileCount = folder.listFiles().length;

        Vector<String> colNames = t.getColNames();
        HashMap<String, Integer> colNameToIndex = new HashMap<>();
        for(int i = 0; i < colNames.size(); i++){
            colNameToIndex.put(colNames.get(i), i);
        }
        for (int i = 0; i < fileCount; i++) { //page number
            Page p = Serializer.deserializePage(t.getTableName(), i);
            for (int j = 0; j < p.getPage().size(); j++) { //tuple number
                Vector<Object> tuple = p.getPage().get(j);
                Vector<Comparable> keyData = new Vector<>();
                for (int k = 0; k < ot.getStrarrColName().length; k++) {
                    String colName = ot.getStrarrColName()[k];
                    int colIndex = colNameToIndex.get(colName);
                    keyData.add((Comparable) tuple.get(colIndex));
                }
                ot.insert(keyData, i, j);
            }
        }
    }
}
