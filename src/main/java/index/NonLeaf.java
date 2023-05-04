package index;

import java.io.IOException;
import java.util.Vector;

public class NonLeaf extends Node {

    Node[] children;
    static final int MAX_CHILDREN = 8;

    public NonLeaf () {
        this.children = new Node[MAX_CHILDREN];
    }


    public Node[] getChildren() {
        return children;
    }

    public int getChildIndex (Vector<Comparable> keyData) {
        int index = 0;
        Vector<Comparable> mid = this.getMid();
        for (int i = 0; i < 3; i++)
            if (keyData.get(i).compareTo(mid.get(i)) <= 0)
                index |= 1 << i;

        return index;
    }

    public void split(Vector<String> typePerCol) throws IOException {
        for (int i = 0; i < NonLeaf.MAX_CHILDREN; i++) {
            Vector<Comparable> newLeft = new Vector<>();
            Vector<Comparable> newRight = new Vector<>();
            for (int j = 0; j < 3; j++) {
                if ((i & (1 << j)) == 0) {
                    newLeft.add(this.leftLimit.get(j));
                    newRight.add(this.mid.get(j));
                } else {
                    newLeft.add(Incrementer.increment(this.mid.get(j)));
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
}
