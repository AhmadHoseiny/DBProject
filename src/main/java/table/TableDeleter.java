package table;

import exceptions.DBAppException;
import helper_classes.Serializer;
import index.Octree;
import page.Page;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.*;

public class TableDeleter {

    static final String directoryPathResourcesData = "src/main/resources/Data/";
    Table table;

    public TableDeleter(Table table) {
        this.table = table;
    }

    public void deleteTuple(Hashtable<String, Object> htblColNameValue) throws DBAppException, IOException, ClassNotFoundException, ParseException {

        File folder = new File(directoryPathResourcesData + table.getTableName() + "/Pages");
        int fileCount = Objects.requireNonNull(folder.listFiles()).length;

        HashSet<Octree> deserializedOctrees = new HashSet<>();
        HashSet<String> alreadyGotten = new HashSet<>();
        for (String index : table.getIndexNames()) {

            if (index == null) {
                continue;
            }

            if (alreadyGotten.contains(index)) {
                continue;
            }

            alreadyGotten.add(index);

            Octree octree = Serializer.deserializeIndex(table, index);
            deserializedOctrees.add(octree);

        }


        //if the table is empty, we delete all records (truncate)
        if (htblColNameValue.isEmpty()) { //all octrees should also be cleared
            for (int i = 0; i < fileCount; i++) {
                File fileToDelete = new File(directoryPathResourcesData + table.getTableName() + "/Pages/Page_" + i + ".ser");
                fileToDelete.delete();
            }
            table.getMinPerPage().clear();
            for (Octree octree : deserializedOctrees) {
                octree.clear();
            }
            for (Octree octree : deserializedOctrees) {
                Serializer.serializeIndex(octree);
            }
            return;
        }


        //a unique row
        if (htblColNameValue.containsKey(table.getClusteringKey())) {

            int index = table.getPageIndex((Comparable) htblColNameValue.get(table.getColNames().get(0)));
            if (index == -1) { // No matching Tuples to be deleted exist
                return;
            }
            Page p = Serializer.deserializePage(table.getTableName(), index);
            int tupleIndex = p.getTupleIndex((Comparable) htblColNameValue.get(table.getColNames().get(0)));
            Vector<Object> tuple = p.getPage().get(tupleIndex);
            boolean nonEmptyPage = p.deleteSingleTuple((Comparable) htblColNameValue.get(table.getColNames().get(0)), table.getColNames(), htblColNameValue);
            table.deleteInOctree(deserializedOctrees, tuple, index, tupleIndex);

            if (nonEmptyPage) {
                Comparable minKey = (Comparable) p.getPage().get(0).get(0);
                table.getMinPerPage().put(index, minKey);
                Serializer.serializePage(p, table.getTableName(), index);
                for (int i = tupleIndex; i < p.getPage().size(); i++) {
                    table.updatePointerInOctree(deserializedOctrees, p.getPage().get(i),
                            index, i + 1, index, i);
                }
            } else {
                File fileToDelete = new File(directoryPathResourcesData + table.getTableName() + "/Pages/Page_" + index + ".ser");
                fileToDelete.delete();
                for (int i = index + 1; i < fileCount; i++) {
                    File fileToRename = new File(directoryPathResourcesData + table.getTableName() + "/Pages/Page_" + i + ".ser");
                    File newFile = new File(directoryPathResourcesData + table.getTableName() + "/Pages/Page_" + (i - 1) + ".ser");
                    fileToRename.renameTo(newFile);
                    table.getMinPerPage().put(i - 1, table.getMinPerPage().get(i));
                }
                table.getMinPerPage().remove(fileCount - 1);
                for (Octree octree : deserializedOctrees) {
                    octree.decrementPageIndicesLargerThanInput(index);
                }
            }
        } else {

            LinkedList<Integer> pagesToDelete = new LinkedList<>();
            int cntDeletedPages = 0;

            //To determine if using Index is applicable
            boolean[] colsToBeTaken = new boolean[3];
            Octree octree = table.getIndexToBeUsed(htblColNameValue, deserializedOctrees, colsToBeTaken);
            if (octree != null) { //we will use the index
                //building the objValues vector
                Vector<Comparable> objValues = new Vector<>();
                for (int i = 0; i < 3; i++) {
                    if (colsToBeTaken[i]) {
                        objValues.add((Comparable) htblColNameValue.get(octree.getStrarrColName()[i]));
                    } else {
                        objValues.add(null);
                    }
                }

                TreeMap<Integer, LinkedList<Integer>> resPointers = octree.searchForDelete(objValues);
                for (int i = 0; i < fileCount; i++) {
                    if (resPointers.containsKey(i)) {
                        Page p = Serializer.deserializePage(table.getTableName(), i);
                        int oldPageIndex = i;
                        int newPageIndex = i - cntDeletedPages;
                        //takes care of
                        // (1) Updating the page index and row index of non deleted tuples
                        // (2) Deleting the deleted tuples from the octree
                        boolean nonEmptyPage = p.deleteAllMatchingTuples(table.getColNames(), htblColNameValue,
                                oldPageIndex, newPageIndex,
                                table, deserializedOctrees);
                        if (nonEmptyPage) {
                            Comparable minKey = (Comparable) p.getPage().get(0).get(0);
                            table.getMinPerPage().put(i, minKey);
                            Serializer.serializePage(p, table.getTableName(), i);
                        } else {
                            cntDeletedPages++;
                            pagesToDelete.add(i);
                        }
                    } else {
                        if (cntDeletedPages > 0)
                            octree.setPageIndicesOfXToY(i, i - cntDeletedPages);
                    }
                }
            } else { //normal linear search
                for (int i = 0; i < fileCount; i++) {
                    Page p = Serializer.deserializePage(table.getTableName(), i);
                    int oldPageIndex = i;
                    int newPageIndex = i - cntDeletedPages;
                    //takes care of
                    // (1) Updating the page index and row index of non deleted tuples
                    // (2) Deleting the deleted tuples from the octree
                    boolean nonEmptyPage = p.deleteAllMatchingTuples(table.getColNames(), htblColNameValue,
                            oldPageIndex, newPageIndex,
                            table, deserializedOctrees);
                    if (nonEmptyPage) {
                        Comparable minKey = (Comparable) p.getPage().get(0).get(0);
                        table.getMinPerPage().put(i, minKey);
                        Serializer.serializePage(p, table.getTableName(), i);
                    } else {
                        cntDeletedPages++;
                        pagesToDelete.add(i);
                    }
                }
            }


            int j = 0;
            for (int i = 0; i < fileCount; i++) {
                if (!pagesToDelete.isEmpty() && i == pagesToDelete.peekFirst()) {
                    File fileToDelete = new File(directoryPathResourcesData + table.getTableName() + "/Pages/Page_" + i + ".ser");
                    fileToDelete.delete();
                    pagesToDelete.removeFirst();
                    j++;
                } else {
                    File fileToRename = new File(directoryPathResourcesData + table.getTableName() + "/Pages/Page_" + i + ".ser");
                    File newFile = new File(directoryPathResourcesData + table.getTableName() + "/Pages/Page_" + (i - j) + ".ser");
                    fileToRename.renameTo(newFile);
                    table.getMinPerPage().put(i - j, table.getMinPerPage().get(i));
                }
            }
            for (int i = 0; i < j; i++) {
                table.getMinPerPage().remove(fileCount - 1 - i);
            }

        }

        for (Octree octree : deserializedOctrees) {
            Serializer.serializeIndex(octree);
        }

    }
}
