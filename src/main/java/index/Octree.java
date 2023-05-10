package index;

import exceptions.DBAppException;
import helper_classes.CSVFileManipulator;
import tables.Table;

import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.util.ArrayDeque;
import java.util.Vector;

public class Octree implements Serializable {
    //While deserializing the octree, its corresponding table should also be deserialized, and call initialize() on it.

    Node root;
    String strTableName;
    String[] strarrColName;
    transient Table table;
    transient Vector<Comparable> minPerCol;
    transient Vector<Comparable> maxPerCol;
    transient Vector<String> typePerCol;

    //only used when creating a new octree
    public Octree(Table t, String[] strarrColName) throws DBAppException, IOException, ClassNotFoundException, ParseException {
        this.strTableName = t.getTableName();
        this.strarrColName = strarrColName;
        table = t; //has to be before initializeIndex
        initializeIndex();
        this.root = new Leaf();
        root.set(minPerCol, maxPerCol, typePerCol);

        CSVFileManipulator.updateUponIndexCreation(t, strarrColName);
    }

    public Node getRoot() {
        return root;
    }

    public String getStrTableName() {
        return strTableName;
    }

    public String[] getStrarrColName() {
        return strarrColName;
    }

    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public Vector<Comparable> getMinPerCol() {
        return minPerCol;
    }

    public Vector<Comparable> getMaxPerCol() {
        return maxPerCol;
    }

    public Vector<String> getTypePerCol() {
        return typePerCol;
    }

    public void initializeIndex() throws DBAppException, IOException, ClassNotFoundException, ParseException {
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


    public Node findNode(Node cur, Vector<Comparable> keyData) {
        if (cur instanceof Leaf)
            return cur;
        else {
            int index = ((NonLeaf) cur).getChildIndex(keyData);
            Node child = ((NonLeaf) cur).getChildren()[index];
            return findNode(child, keyData);
        }
    }

//    public Node findAllNodes(Node cur) {
//        if (cur instanceof Leaf)
//            return cur;
//        else {
//
//        }
//    }


    public void updatePointer(Vector<Comparable> keyData, int oldPageIndex, int oldRowIndex, int newPageIndex, int newRowIndex) {
        Node cur = findNode(root, keyData);
        ((Leaf) cur).updatePointer(keyData, oldPageIndex, oldRowIndex, newPageIndex, newRowIndex);
    }

    public void insert(Vector<Comparable> keyData, int pageIndex, int rowIndex) throws IOException {

        OctreeInserter octreeInserter = new OctreeInserter(this);
        octreeInserter.insert(keyData, pageIndex, rowIndex);

    }


    public void printIndexDFS() {
        printIndexDFS(root);
    }

    public void printIndexDFS(Node cur) {
        if (cur == null)
            return;
        System.out.println(cur);
        if (cur instanceof NonLeaf) {
            for (Node child : ((NonLeaf) cur).getChildren()) {
                printIndexDFS(child);
            }
        }
    }

    public void printIndexBFS() {
        ArrayDeque<Node> q = new ArrayDeque();
        q.add(root);
        while(!q.isEmpty()){
            int cnt = 0;
            ArrayDeque<Node> nxtLevel = new ArrayDeque();
            while(!q.isEmpty()){
                Node cur = q.pollFirst();
                System.out.print(cur +" !! ");
                cnt++;
                if(cnt==8){
                    System.out.println(" *** ");
                }
                if(cur instanceof Leaf)
                    continue;
                for(Node child : ((NonLeaf) cur).getChildren()){
                    nxtLevel.addLast(child);
                }
            }
            System.out.println();
            q = nxtLevel;
        }

    }
}
