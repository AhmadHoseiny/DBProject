package helper_classes;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class ReadConfigFile {

    public static Properties getProperties() throws IOException {
        Properties prop = new Properties();
        FileInputStream ip = new FileInputStream("src/main/resources/DBApp.config");
        prop.load(ip);
        return prop;
    }

    public static int getMaximumRowsCountInTablePage() throws IOException {
        Properties prop = ReadConfigFile.getProperties();
        return Integer.parseInt(prop.getProperty("MaximumRowsCountinTablePage"));
    }

    public static int getMaximumEntriesInOctreeNode() throws IOException {
        Properties prop = ReadConfigFile.getProperties();
        return Integer.parseInt(prop.getProperty("MaximumEntriesinOctreeNode"));
    }
}
