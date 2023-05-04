package index;

import exceptions.*;
import helper_classes.*;
import tables.*;

import java.io.*;
import java.text.ParseException;
import java.util.*;

public class Octree implements Serializable {
    //While deserializing the octree, its corresponding table should also be deserialized, and call initialize() on it.

    Node root;
    String strTableName;
    String[] strarrColName;
    transient Table table;
    transient Vector<Comparable> minPerCol;
    transient Vector<Comparable> maxPerCol;
    transient Vector<String> typePerCol;

    public Octree (String strTableName, String[] strarrColName) throws DBAppException, IOException, ClassNotFoundException, ParseException {

        this.strTableName = strTableName;
        this.strarrColName = strarrColName;
        table = Serializer.deserializeTable(strTableName);
        initialize();
        this.root = new Leaf();
        root.set(minPerCol, maxPerCol, typePerCol);

    }


    public void initialize () throws DBAppException, IOException, ClassNotFoundException {
        minPerCol = new Vector<>();
        maxPerCol = new Vector<>();
        typePerCol = new Vector<>();
        for (int i = 0; i < strarrColName.length; i++) {
            int colIndex = table.getColNames().indexOf(strarrColName[i]);
            if (colIndex == -1)
                throw new DBAppException("Column " + strarrColName[i] + " does not exist in table " + strTableName);
            minPerCol.add(table.getColMin().get(colIndex));
            maxPerCol.add(table.getColMax().get(colIndex));
            typePerCol.add(table.getColTypes().get(colIndex));
        }
    }





    public Node findNode (Node cur, Vector<Comparable> keyData) {
        if (cur instanceof Leaf)
            return cur;
        else {
            int index = ((NonLeaf) cur).getChildIndex(keyData);
            Node child = ((NonLeaf) cur).getChildren()[index];
            return findNode(child, keyData);
        }


    }

    public void insert (Vector<Comparable> keyData, int pageIndex, int rowIndex) {

        OctreeInserter octreeInserter = new OctreeInserter(this);
        octreeInserter.insert(keyData, pageIndex, rowIndex);

    }



}
