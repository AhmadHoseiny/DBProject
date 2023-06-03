package table;

import exceptions.DBAppException;
import helper_classes.Serializer;
import index.Octree;
import page.Page;

import java.io.IOException;
import java.text.ParseException;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Vector;

public class TableUpdater {

    Table table;

    public TableUpdater(Table table) {
        this.table = table;
    }

    public void updateTuple(Comparable clusteringKeyVal, Hashtable<String, Object> htblColNameValue) throws IOException, ClassNotFoundException, DBAppException, ParseException {
        int index = table.getPageIndex(clusteringKeyVal);
        if (index == -1) //clustering key does not exist
            return;

        Page p = Serializer.deserializePage(table.getTableName(), index);
        int rowIndex = p.getTupleIndex(clusteringKeyVal);
        Vector<Object> oldTuple = (Vector<Object>) p.getPage().get(rowIndex).clone();
        p.updateTuple(clusteringKeyVal, table.getColNames(), htblColNameValue);
        Vector<Object> newTuple = p.getPage().get(rowIndex);
        Serializer.serializePage(p, table.getTableName(), index);

        //update in octree
        HashSet<Octree> deserializedOctrees = new HashSet<>();
        HashSet<String> alreadyGotten = new HashSet<>();
        for (String indexN : table.getIndexNames()) {

            if (indexN == null) {
                continue;
            }

            if (alreadyGotten.contains(indexN)) {
                continue;
            }

            alreadyGotten.add(indexN);

            Octree octree = Serializer.deserializeIndex(table, indexN);
            deserializedOctrees.add(octree);

        }

        table.deleteInOctree(deserializedOctrees, oldTuple, index, rowIndex);
        table.insertInOctree(deserializedOctrees, newTuple, index, rowIndex);

        for (Octree octree : deserializedOctrees) {
            Serializer.serializeIndex(octree);
        }
    }

}
