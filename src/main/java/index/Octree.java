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

    public Node findNode (Node cur, Comparable x, Comparable y, Comparable z) {
        if (cur instanceof Leaf)
            return cur;
        else {
            int index = getChildIndex(cur, x, y, z);
            Node child = ((NonLeaf) cur).getChildren()[index];
            return findNode(child, x, y, z);
        }


    }

    public int getChildIndex (Node cur, Comparable x, Comparable y, Comparable z) {
        int index = 0;
        Vector<Comparable> mid = cur.getMid();
        for (int i = 0; i < 3; i++)
            if (x.compareTo(mid.get(i)) <= 0)
                index |= 1 << i;

        return index;
    }

    public void insert (Comparable x, Comparable y, Comparable z, int pageIndex, int rowIndex) {

        Node cur = findNode(root, x, y, z);
        insertHelper(x, y, z, pageIndex, rowIndex, cur);

    }

    public void insert (Comparable x, Comparable y, Comparable z, int pageIndex, int rowIndex, Node optimizedRoot) {

        Node cur = findNode(optimizedRoot, x, y, z);
        insertHelper(x, y, z, pageIndex, rowIndex, cur);

    }

    public void insertHelper (Comparable x, Comparable y, Comparable z, int pageIndex, int rowIndex, Node cur) {

        if (((Leaf) cur).hasData) {
            if (x == ((Leaf) cur).x && y == ((Leaf) cur).y && z == ((Leaf) cur).z) { // if the point already exists in the tree (duplicate)
                ((Leaf) cur).add(pageIndex, rowIndex);
                return;
            }
            Node newNode = new NonLeaf();
            newNode.set(cur.leftLimit, cur.rightLimit, typePerCol);
            for (int i = 0; i < NonLeaf.MAX_CHILDREN; i++) {
                Vector<Comparable> newLeft = new Vector<>();
                Vector<Comparable> newRight = new Vector<>();
                for (int j = 0; j < 3; j++)
                    if ((i & (1 << j)) == 0) {
                        newLeft.add(cur.leftLimit.get(j));
                        newRight.add(cur.mid.get(j));
                    } else {
                        newLeft.add(increment(cur.mid.get(j)));
                        newRight.add(cur.rightLimit.get(j));
                    }
                Node newChild = new Leaf();
                newChild.set(newLeft, newRight, typePerCol);
                newChild.setParent(newNode);
                newChild.setIndexInParent(i);
                ((NonLeaf) newNode).getChildren()[i] = newChild;
            }
            newNode.setParent(cur.getParent());
            newNode.setIndexInParent(cur.getIndexInParent());

            if (cur.getParent() == null)
                root = newNode;
            else
                ((NonLeaf) cur.getParent()).getChildren()[cur.getIndexInParent()] = newNode;

            for (int i = 0; i < ((Leaf) cur).pageIndex.size(); i++)
                this.insert(((Leaf) cur).x, ((Leaf) cur).y, ((Leaf) cur).z, ((Leaf) cur).pageIndex.get(i), ((Leaf) cur).rowIndex.get(i), newNode);
            this.insert(x, y, z, pageIndex, rowIndex, newNode);

        } else {
            ((Leaf) cur).x = x;
            ((Leaf) cur).y = y;
            ((Leaf) cur).z = z;
            ((Leaf) cur).add(pageIndex, rowIndex);
            ((Leaf) cur).hasData = true;
        }

    }

    public static Comparable increment( Comparable x) {
        if (x instanceof Integer)
            return (Integer) x + 1;
        else if (x instanceof Double)
            return (Double) x + 1;
        else if (x instanceof String)
            return ((String) x).charAt(((String) x).length() - 1) + 1 + "";
        else if (x instanceof Date)
            return new Date(((Date) x).getTime() + 1);
        else
            return null;
    }

}
