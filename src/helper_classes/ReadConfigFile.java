package helper_classes;

import java.io.*;
import java.util.*;
public class ReadConfigFile {

    public static Properties getProperties() throws IOException {
        Properties prop=new Properties();
        FileInputStream ip = new FileInputStream("src/DBApp.config");
        prop.load(ip);
        return prop;
    }

}
