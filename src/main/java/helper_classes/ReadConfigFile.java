package helper_classes;

import java.io.*;
import java.util.*;

public class ReadConfigFile {

    public static Properties getProperties() throws IOException {
        Properties prop = new Properties();
        FileInputStream ip = new FileInputStream("src/main/resources/DBApp.config");
        prop.load(ip);
        return prop;
    }

    public static int getMaximumRowsCountInTablePage() throws IOException {
        Properties prop = ReadConfigFile.getProperties();
        return Integer.parseInt(prop.getProperty("MaximumRowsCountInTablePage"));
    }

}
