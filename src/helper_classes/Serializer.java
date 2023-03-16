package helper_classes;
import java.io.*;
import java.util.*;
import tables.*;
public class Serializer {

    public static void serializePage(Page p, String tableName, int pageIndex)
            throws IOException {
        FileOutputStream fileOut =
                new FileOutputStream("Serialized Files/" +
                        tableName+ "/Page_" + pageIndex + ".ser");
        ObjectOutputStream out = new ObjectOutputStream(fileOut);
        out.writeObject(p);
        out.close();
        fileOut.close();
    }

    public static Page deserializePage(String tableName, int pageIndex)
            throws IOException, ClassNotFoundException {
        FileInputStream fileIn = new FileInputStream("Serialized Files/" +
                tableName+ "/Page_" + pageIndex + ".ser");
        ObjectInputStream in = new ObjectInputStream(fileIn);
        Page p = (Page) in.readObject();
        in.close();
        fileIn.close();
        return p;
    }
}
