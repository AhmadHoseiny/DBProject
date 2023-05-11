package index;

import helper_classes.ReadConfigFile;

import java.io.IOException;
import java.util.Vector;

public class OctreeDeleter {
    Octree octree;
    public OctreeDeleter(Octree octree) {
        this.octree = octree;
    }

    public void delete(Vector<Comparable> keyData, int pageIndex, int rowIndex) throws IOException {

        Node cur = octree.findNode(octree.root, keyData);
        deleteHelper(keyData, pageIndex, rowIndex, cur);
    }
    public void deleteHelper(Vector<Comparable> keyData, int pageIndex, int rowIndex, Node cur) throws IOException {

        //if the leaf had space, insert the data into it (it also includes duplicates handling)
        ((Leaf) cur).deleteData(keyData, pageIndex, rowIndex);


        Node parent = cur.getParent();

        //the leaf was the root, so nothing to do
        if (parent == null) {
            return;
        }
        Integer cntChildren = ((NonLeaf) parent).countKeysInAllChildren();
        int maximumEntriesInOctreeNode = ReadConfigFile.getMaximumEntriesInOctreeNode();
        //the nonLeaf has enough keys in all its children(strictly more than max), so nothing to do
        if(cntChildren==null || Integer.compare(cntChildren, maximumEntriesInOctreeNode)>0){
            return;
        }

        //the nonLeaf has less than or equal to max keys in all its children,
        // so it needs to be merged with its siblings
        Leaf merged = ((NonLeaf) parent).mergeChildren();
        merged.set(parent.leftLimit, parent.rightLimit, octree.typePerCol);

        Node grandParent = parent.getParent();
        if(grandParent==null){
            octree.root = merged;
            return;
        }

        //tell the leaf that grandparent is its parent
        merged.setParent(grandParent);
        merged.setIndexInParent(parent.getIndexInParent());

        //tell the grandparent that the leaf is its child
        ((NonLeaf) grandParent).getChildren()[parent.getIndexInParent()] = merged;

    }

    public void decrementPageIndicesLargerThanInput(int pageIndex) {
       dfs(octree.root, pageIndex);
    }
    public void dfs(Node cur, int pageIndex) {

        if (cur instanceof Leaf){
            ((Leaf) cur).decrementPageIndicesLargerThanInput(pageIndex);
            return;
        }

        for(Node child : ((NonLeaf) cur).getChildren()){
            dfs(child, pageIndex);
        }

    }

}
