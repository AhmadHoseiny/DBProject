package index;

import helper_classes.ReadConfigFile;

import java.io.IOException;
import java.util.Vector;

public class OctreeInserter {
    Octree octree;

    public OctreeInserter(Octree octree) {
        this.octree = octree;
    }

    public void insert(Vector<Comparable> keyData, int pageIndex, int rowIndex) throws IOException {

        Node cur = octree.findNode(octree.root, keyData);
        insertHelper(keyData, pageIndex, rowIndex, cur);

    }

    public void insertOptimized(Vector<Comparable> keyData, int pageIndex, int rowIndex, Node optimizedRoot) throws IOException {

        Node cur = octree.findNode(optimizedRoot, keyData);
        insertHelper(keyData, pageIndex, rowIndex, cur);

    }

    public void insertHelper(Vector<Comparable> keyData, int pageIndex, int rowIndex, Node cur) throws IOException {

        //if the leaf had space, insert the data into it (it also includes duplicates handling)
        if (((Leaf) cur).insertData(keyData, pageIndex, rowIndex)) {
            return;
        }
//        System.out.println("? " + keyData +"__" + cur);
        //otherwise, the node needs to be split

        // create a newNode and set its limits to the limits of the current node
        //this newNode will replace the cur node
        Node newNode = new NonLeaf();
        newNode.set(cur.leftLimit, cur.rightLimit, octree.typePerCol);

        //create all the children of the newNode
        ((NonLeaf) newNode).split(octree.typePerCol);

        //tell the newNode who its parent is and what is its index in the parent's children array
        newNode.setParent(cur.getParent());
        newNode.setIndexInParent(cur.getIndexInParent());

        //tell the parent of the newNode that newNode is its child,
        //or if it is the root, set the root to be the newNode
        if (cur.getParent() == null)
            octree.root = newNode;
        else
            ((NonLeaf) cur.getParent()).getChildren()[cur.getIndexInParent()] = newNode;

        //Insert all data (including duplicates) that were in cur into newNode's children
        int maximumEntriesInOctreeNode = ReadConfigFile.getMaximumEntriesInOctreeNode();
        for (int i = 0; i < maximumEntriesInOctreeNode; i++) {
            Vector<Comparable> keyDataInCur = ((Leaf) cur).keyDataVector.get(i);
            Vector<Integer> pageIndexInCur = ((Leaf) cur).pageIndexVector.get(i);
            for (int j = 0; j < pageIndexInCur.size(); j++) {
                this.insertOptimized(keyDataInCur, pageIndexInCur.get(j), ((Leaf) cur).rowIndexVector.get(i).get(j), newNode);
            }
        }
        //Finally, insert the new keyData into newNode's children
        this.insertOptimized(keyData, pageIndex, rowIndex, newNode);

    }


}
