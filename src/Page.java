import java.io.*;
import java.util.*;

import exceptions.DBAppException;
import helper_classes.*;
public class Page implements Serializable{
    Vector<Vector<Object>> page;
    public Page() {
        page = new Vector<>();
    }

    //returns true if inserting is successful without exceeding max no. of rows in a page
    public boolean insert(Vector<String> colNames , Hashtable<String, Object> htblColNameValue)
            throws IOException, DBAppException {
        int maxSize = ReadConfigFile.getMaximumRowsCountInTablePage();
        if(page.size()>=maxSize)
            return false;
        Vector<Object> tuple = new Vector<>();
        for(String colName : colNames){
            tuple.add(htblColNameValue.get(colName));
        }
        page.add(tuple);
        return true;
    }

}
