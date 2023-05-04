package index;

import java.util.Vector;

public class OctreeInserter {
    Octree octree;
    public OctreeInserter(Octree octree) {
        this.octree = octree;
    }
    public void insert (Vector<Comparable> keyData, int pageIndex, int rowIndex) {

        Node cur = octree.findNode(octree.root, keyData);
        insertHelper(keyData, pageIndex, rowIndex, cur);

    }

    public void insertOptimized (Vector<Comparable> keyData, int pageIndex, int rowIndex, Node optimizedRoot) {

        Node cur = octree.findNode(optimizedRoot, keyData);
        insertHelper(keyData, pageIndex, rowIndex, cur);

    }

    public void insertHelper (Vector<Comparable> keyData, int pageIndex, int rowIndex, Node cur) {

        //if the leaf is empty or the keyData is a duplicate, keyData is directly inserted
        if(!((Leaf) cur).hasData || isDuplicateKeyData(keyData, cur)){
            ((Leaf) cur).insertData(keyData, pageIndex, rowIndex);
        }
        else{ //otherwise, the node is split

            //create a newNode and set its limits to the limits of the current node
            //this newNode will replace the cur node
            Node newNode = new NonLeaf();
            newNode.set(cur.leftLimit, cur.rightLimit, octree.typePerCol);

            //create all the children of the newNode
            ((NonLeaf)newNode).split(octree.typePerCol);

            //tell the newNode who its parent is and what is its index in the parent's children array
            newNode.setParent(cur.getParent());
            newNode.setIndexInParent(cur.getIndexInParent());

            //tell the parent of the newNode that newNode is its child,
            //or if it is the root, set the root to be the newNode
            if (cur.getParent() == null)
                octree.root = newNode;
            else
                ((NonLeaf) cur.getParent()).getChildren()[cur.getIndexInParent()] = newNode;

            //Insert all data (they may be more than 1 (duplicates)) that were in cur into newNode's children
            for (int i = 0; i < ((Leaf) cur).pageIndex.size(); i++)
                this.insertOptimized(((Leaf) cur).keyData, ((Leaf) cur).pageIndex.get(i), ((Leaf) cur).rowIndex.get(i), newNode);

            //Finally, insert the new keyData into newNode's children
            this.insertOptimized(keyData, pageIndex, rowIndex, newNode);
        }

    }
    public boolean isDuplicateKeyData(Vector<Comparable> keyData, Node cur) {
        boolean isDuplicate = true;
        for(int i=0 ; i<3 ; i++){
            isDuplicate &= keyData.get(i).equals(((Leaf) cur).keyData.get(i));
        }
        return isDuplicate;
    }

}
