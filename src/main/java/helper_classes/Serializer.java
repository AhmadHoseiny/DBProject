package helper_classes;

import exceptions.DBAppException;
import index.Octree;
import page.Page;
import table.Table;

import java.io.*;
import java.text.ParseException;

public class Serializer {

    static final String directoryPathResourcesData = "src/main/resources/Data/";

    public static void serializePage(Page p, String tableName, int pageIndex)
            throws IOException {
        FileOutputStream fileOut =
                new FileOutputStream(directoryPathResourcesData +
                        tableName + "/Pages/Page_" + pageIndex + ".ser");
        ObjectOutputStream out = new ObjectOutputStream(fileOut);
        out.writeObject(p);
        out.close();
        fileOut.close();
    }

    public static Page deserializePage(String tableName, int pageIndex)
            throws IOException, ClassNotFoundException {
        FileInputStream fileIn = new FileInputStream(directoryPathResourcesData +
                tableName + "/Pages/Page_" + pageIndex + ".ser");
        ObjectInputStream in = new ObjectInputStream(fileIn);
        Page p = (Page) in.readObject();
        in.close();
        fileIn.close();

        return p;
    }

    public static void serializeTable(Table t, String tableName) throws IOException {

        DirectoryCreator.createDirectory(directoryPathResourcesData + tableName);
        DirectoryCreator.createDirectory(directoryPathResourcesData + tableName + "/Pages");
        DirectoryCreator.createDirectory(directoryPathResourcesData + tableName + "/Indices");
        FileOutputStream fileOut =
                new FileOutputStream(directoryPathResourcesData +
                        tableName + ".ser");
        ObjectOutputStream out = new ObjectOutputStream(fileOut);
        out.writeObject(t);
        out.close();
        fileOut.close();
    }

    public static Table deserializeTable(String tableName)
            throws IOException, ClassNotFoundException, DBAppException, ParseException {
        FileInputStream fileIn = new FileInputStream(directoryPathResourcesData +
                tableName + ".ser");
        ObjectInputStream in = new ObjectInputStream(fileIn);
        Table t = (Table) in.readObject();
        if (t == null)
            throw new DBAppException("Table does not exist");
        t.initializeTable();
        in.close();
        fileIn.close();

        return t;
    }


    public static void serializeIndex(Octree ot) throws IOException {
        String tableName = ot.getStrTableName();
        String indexName = IndexNameGetter.getIndexName(ot.getStrarrColName());
        FileOutputStream fileOut =
                new FileOutputStream(directoryPathResourcesData +
                        tableName + "/Indices/" + indexName + ".ser");
        ObjectOutputStream out = new ObjectOutputStream(fileOut);
        out.writeObject(ot);
        out.close();
        fileOut.close();
    }

    public static Octree deserializeIndex(Table t, String indexName)
            throws IOException, ClassNotFoundException, DBAppException, ParseException {
        FileInputStream fileIn = new FileInputStream(directoryPathResourcesData +
                t.getTableName() + "/Indices/" + indexName + ".ser");
        ObjectInputStream in = new ObjectInputStream(fileIn);
        Octree ot = (Octree) in.readObject();
        if (ot == null)
            throw new DBAppException("Index does not exist");
        ot.setTable(t); //has to be made before initializeIndex
        ot.initializeIndex();
        in.close();
        fileIn.close();

        return ot;
    }

}
