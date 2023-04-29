package index;

public class NonLeaf extends Node {

    Node[] children;
    static final int MAX_CHILDREN = 8;

    public NonLeaf () {
        this.children = new Node[MAX_CHILDREN];
    }


    public Node[] getChildren() {
        return children;
    }
}
