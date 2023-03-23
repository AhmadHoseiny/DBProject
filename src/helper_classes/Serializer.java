package helper_classes;
import java.io.*;
import java.util.*;

import exceptions.DBAppException;
import tables.*;
public class Serializer {

    public static void serializePage(Page p, String tableName, int pageIndex)
            throws IOException {
        FileOutputStream fileOut =
                new FileOutputStream("Serialized Database/" +
                        tableName+ "/Page_" + pageIndex + ".ser");
        ObjectOutputStream out = new ObjectOutputStream(fileOut);
        out.writeObject(p);
        out.close();
        fileOut.close();
    }

    public static Page deserializePage(String tableName, int pageIndex)
            throws IOException, ClassNotFoundException {
        FileInputStream fileIn = new FileInputStream("Serialized Database/" +
                tableName + "/Page_" + pageIndex + ".ser");
        ObjectInputStream in = new ObjectInputStream(fileIn);
        Page p = (Page) in.readObject();
        in.close();
        fileIn.close();

        return p;
    }

    public static void serializeTable(Table t, String tableName) throws IOException {

        String directoryPath = "Serialized Database/" + tableName;
        File directory = new File(directoryPath);
        if(!directory.isDirectory())
            new File(directoryPath).mkdirs();

        FileOutputStream fileOut =
                new FileOutputStream("Serialized Database/" +
                        tableName + ".ser");
        ObjectOutputStream out = new ObjectOutputStream(fileOut);
        out.writeObject(t);
        out.close();
        fileOut.close();
    }

    public static Table deserializeTable(String tableName)
            throws IOException, ClassNotFoundException, DBAppException {
        FileInputStream fileIn = new FileInputStream("Serialized Database/" +
                tableName + ".ser");
        ObjectInputStream in = new ObjectInputStream(fileIn);
        Table t = (Table) in.readObject();
        t.setTable(new Vector<>());
        in.close();
        fileIn.close();

        return t;
    }

//    public static void serializeDetails(String tableName, HashMap<Integer, String> data) throws IOException {
//        String directoryPath = "Serialized Database/" + tableName;
//        File directory = new File(directoryPath);
//
//        FileOutputStream fileOut =
//                new FileOutputStream("Serialized Database/" +
//                        tableName + "/" +  "Data.ser");
//        ObjectOutputStream out = new ObjectOutputStream(fileOut);
//        out.writeObject(data);
//        out.close();
//        fileOut.close();
//    }

//    public static HashMap<Integer, String> deserializeDetails(String tableName) throws IOException, ClassNotFoundException {
//        FileInputStream fileIn = new FileInputStream("Serialized Database/" +
//                tableName + "/" + "Data.ser");
//        ObjectInputStream in = new ObjectInputStream(fileIn);
//        HashMap<Integer, String> data = (HashMap<Integer, String>) in.readObject();
//        in.close();
//        fileIn.close();
//
//        return data;
//    }

}
