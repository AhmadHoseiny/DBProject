package index;

import exceptions.DBAppException;
import helper_classes.GenericComparator;

import java.util.LinkedList;
import java.util.TreeMap;
import java.util.Vector;

public class OctreeSearcher {

    Octree octree;
    LinkedList<Leaf> leavesForSelect;
    LinkedList<Leaf> leavesForDelete;

    public OctreeSearcher(Octree octree) {
        this.octree = octree;
    }

    public TreeMap<Integer, LinkedList<Integer>> searchForSelect(Vector<Comparable> objValues, Vector<String> operators) throws DBAppException {
        leavesForSelect = new LinkedList<>();
        dfsForSelect(octree.getRoot(), objValues, operators);
        TreeMap<Integer, LinkedList<Integer>> resPointers = new TreeMap<>();
        for (Leaf leaf : leavesForSelect) {
            for (int i = 0; i < leaf.pageIndexVector.size(); i++) {
                for (int j = 0; j < leaf.pageIndexVector.get(i).size(); j++) {
                    int pageIndex = leaf.pageIndexVector.get(i).get(j);
                    int rowIndex = leaf.rowIndexVector.get(i).get(j);
                    if (!resPointers.containsKey(pageIndex)) {
                        resPointers.put(pageIndex, new LinkedList<>());
                    }
                    resPointers.get(pageIndex).add(rowIndex);
                }
            }
        }
        return resPointers;
    }

    public void dfsForSelect(Node cur, Vector<Comparable> objValues, Vector<String> operators) throws DBAppException {
        if (cur instanceof Leaf) {
            leavesForSelect.add((Leaf) cur);
            return;
        }
        LinkedList<Integer> masks = new LinkedList<>();
        //maybe another idea to be given a shot --> loop on the 8 masks and check if they are needed or not
        genMasksForSelect(0, (NonLeaf) cur, objValues, operators, 0, masks);
        for (int mask : masks) {
            dfsForSelect(((NonLeaf) cur).getChildren()[mask], objValues, operators);
        }
    }

    public void genMasksForSelect(int idx, NonLeaf cur,
                                  Vector<Comparable> objValues, Vector<String> operators,
                                  int mask, LinkedList<Integer> resMasks) throws DBAppException {
        if (idx == 3) {
            resMasks.addLast(mask);
            return;
        }
        switch (operators.get(idx)) {
            case "=":
                int newMask = mask;
                if (GenericComparator.compare(objValues.get(idx), cur.getMid().get(idx)) > 0)
                    newMask |= (1 << idx);
                genMasksForSelect(idx + 1, cur, objValues, operators, newMask, resMasks);
                break;
            case ">":
                genMasksForSelect(idx + 1, cur, objValues, operators, mask | (1 << idx), resMasks);
                if (GenericComparator.compare(objValues.get(idx), cur.getMid().get(idx)) < 0) {
                    genMasksForSelect(idx + 1, cur, objValues, operators, mask, resMasks);
                }
                break;
            case ">=":
                genMasksForSelect(idx + 1, cur, objValues, operators, mask | (1 << idx), resMasks);
                if (GenericComparator.compare(objValues.get(idx), cur.getMid().get(idx)) <= 0) {
                    genMasksForSelect(idx + 1, cur, objValues, operators, mask, resMasks);
                }
                break;
            case "<":
            case "<=":
                genMasksForSelect(idx + 1, cur, objValues, operators, mask, resMasks);
                if (GenericComparator.compare(objValues.get(idx), cur.getMid().get(idx)) > 0) {
                    genMasksForSelect(idx + 1, cur, objValues, operators, mask | (1 << idx), resMasks);
                }
                break;
            case "!=":
                System.out.println("!= not supported in octree    Wrong query");
                break;
            default:
                throw new DBAppException("Invalid operator");

        }
    }

    public TreeMap<Integer, LinkedList<Integer>> searchForDelete(Vector<Comparable> objValues) {
        leavesForDelete = new LinkedList<>();
        dfsForDelete(octree.getRoot(), objValues);
        TreeMap<Integer, LinkedList<Integer>> resPointers = new TreeMap<>();
        for (Leaf leaf : leavesForDelete) {
            for (int i = 0; i < leaf.pageIndexVector.size(); i++) {
                for (int j = 0; j < leaf.pageIndexVector.get(i).size(); j++) {
                    int pageIndex = leaf.pageIndexVector.get(i).get(j);
                    int rowIndex = leaf.rowIndexVector.get(i).get(j);
                    if (!resPointers.containsKey(pageIndex)) {
                        resPointers.put(pageIndex, new LinkedList<>());
                    }
                    resPointers.get(pageIndex).add(rowIndex);
                }
            }
        }
        return resPointers;

    }
    public void dfsForDelete(Node cur, Vector<Comparable> objValues) {
        if (cur instanceof Leaf) {
            leavesForDelete.add((Leaf) cur);
            return;
        }
        LinkedList<Integer> masks = new LinkedList<>();
        genMasksForDelete(0, (NonLeaf) cur, objValues, 0, masks);
        for (int mask : masks) {
            dfsForDelete(((NonLeaf) cur).getChildren()[mask], objValues);
        }
    }
    public void genMasksForDelete(int idx, NonLeaf cur,
                                  Vector<Comparable> objValues,
                                  int mask, LinkedList<Integer> resMasks){
        if (idx == 3) {
            resMasks.addLast(mask);
            return;
        }
        if(objValues.get(idx) == null){ //to support partial queries
            genMasksForDelete(idx + 1, cur, objValues, mask, resMasks);
            genMasksForDelete(idx + 1, cur, objValues, mask | (1 << idx), resMasks);
        }
        else{
            int newMask = mask;
            if (GenericComparator.compare(objValues.get(idx), cur.getMid().get(idx)) > 0)
                newMask |= (1 << idx);
            genMasksForDelete(idx + 1, cur, objValues, newMask, resMasks);
        }
    }
}
