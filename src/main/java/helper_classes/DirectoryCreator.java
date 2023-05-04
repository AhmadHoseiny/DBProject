package helper_classes;

import java.io.File;

public class DirectoryCreator {
    public static void createDirectory(String directoryPath){
        File directory = new File(directoryPath);
        if (!directory.isDirectory())
            new File(directoryPath).mkdirs();
    }
}
