package index;

import helper_classes.GenericComparator;

import java.io.IOException;
import java.util.Vector;

public class NonLeaf extends Node {

    static final int MAX_CHILDREN = 8;
    Node[] children;

    public NonLeaf() {
        this.children = new Node[MAX_CHILDREN];
    }


    public Node[] getChildren() {
        return children;
    }

    public int getChildIndex(Vector<Comparable> keyData) {
        int index = 0;
        Vector<Comparable> mid = this.getMid();
//        System.out.println("mid: " + mid + " keyData: " + keyData);
        for (int i = 0; i < 3; i++)
            if (GenericComparator.compare(keyData.get(i), mid.get(i)) > 0)
                index |= 1 << i;

        return index;
    }

    public void split(Vector<String> typePerCol) throws IOException {
//        System.out.println(this.leftLimit + " " + this.mid + " " + this.rightLimit);
        for (int i = 0; i < NonLeaf.MAX_CHILDREN; i++) {
            Vector<Comparable> newLeft = new Vector<>();
            Vector<Comparable> newRight = new Vector<>();
            for (int j = 0; j < 3; j++) {
                if ((i & (1 << j)) == 0) {
                    newLeft.add(this.leftLimit.get(j));
                    newRight.add(this.mid.get(j));
                } else {
//                    newLeft.add(Incrementer.increment(this.mid.get(j)));
                    newLeft.add(this.mid.get(j));
                    newRight.add(this.rightLimit.get(j));
                }
            }
            Node newChild = new Leaf();
            newChild.set(newLeft, newRight, typePerCol);
            newChild.setParent(this);
            newChild.setIndexInParent(i);
            this.getChildren()[i] = newChild;
        }
    }

    public String toString() {
        return super.toString() + "}";
    }

    public Integer countKeysInAllChildren() {
        int count = 0;
        for (int i = 0; i < MAX_CHILDREN; i++) {
            Node child = this.getChildren()[i];
            if (child instanceof NonLeaf)
                return null;
            count += ((Leaf) child).keyDataVector.size();
        }
        return count;
    }

    public Leaf mergeChildren() throws IOException {
        Leaf merged = new Leaf();
        for (int i = 0; i < MAX_CHILDREN; i++) {
            Node child = this.getChildren()[i];
            merged.keyDataVector.addAll(((Leaf) child).keyDataVector);
            merged.pageIndexVector.addAll(((Leaf) child).pageIndexVector);
            merged.rowIndexVector.addAll(((Leaf) child).rowIndexVector);
        }
        for (int i = 0; i < merged.keyDataVector.size(); i++) {
            merged.hasData.setElementAt(true, i);
        }
        return merged;
    }
}
