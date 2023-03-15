import helper_classes.ReadConfigFile;

import java.util.Properties;
import java.io.*;
import java.util.*;
import helper_classes.*;
public class DBApp {

    public static void main(String[] args) throws IOException{
        Properties prop = ReadConfigFile.getProperties();
        System.out.println(prop.getProperty("MaximumRowsCountInTablePage = 200"));
    }
}
