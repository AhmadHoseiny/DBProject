package helper_classes;

import java.io.*;
import java.util.*;
public class ReadConfigFile {

    public static Properties getProperties() throws IOException {
        Properties prop=new Properties();
//        FileInputStream ip = new FileInputStream("D://GUC/6th semester/Databases/Project/DBProject/" +
//                "src/DBApp.config");
        FileInputStream ip = new FileInputStream("src/DBApp.config");
        prop.load(ip);
        return prop;
    }

}
