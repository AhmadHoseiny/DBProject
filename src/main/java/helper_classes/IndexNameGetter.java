package helper_classes;

import java.io.FileNotFoundException;

public class IndexNameGetter {
    public static String getIndexName(String[] strarrColName) throws FileNotFoundException {
        StringBuilder sb = new StringBuilder();
        for (String x : strarrColName) {
            sb.append(x.substring(0, 1).toUpperCase() + x.substring(1).toLowerCase());
        }
        sb.append("Index");
        return sb.toString();
    }
}
