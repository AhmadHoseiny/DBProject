package table;

import exceptions.DBAppException;
import helper_classes.ReadConfigFile;
import helper_classes.Serializer;
import index.Octree;
import page.Page;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Objects;
import java.util.Vector;

public class TableInserter {

    static final String directoryPathResourcesData = "src/main/resources/Data/";
    Table table;

    public TableInserter(Table table) {
        this.table = table;
    }

    public void insertTuple(Hashtable<String, Object> htblColNameValue) throws IOException, ClassNotFoundException, DBAppException, ParseException {

        String directoryPath = directoryPathResourcesData + table.getTableName() + "/Pages";
        File directory = new File(directoryPath);
        int fileCount = Objects.requireNonNull(directory.listFiles()).length;

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

//       if the table is empty
        if (fileCount == 0) {

            Page page = new Page();
            table.getTable().add(page);
            page.insertTuple(table.getColNames(), htblColNameValue);
            table.getMinPerPage().put(0, (Comparable) htblColNameValue.get(table.getClusteringKey()));
            Serializer.serializePage(page, table.getTableName(), 0);
            table.insertInOctree(deserializedOctrees, page.getPage().get(0), 0, 0);
            for (Octree octree : deserializedOctrees) {
                Serializer.serializeIndex(octree);
            }
            return;

        }

        int maxPageSize = ReadConfigFile.getMaximumRowsCountInTablePage();
        int pageIndex = table.getPageIndex((Comparable) htblColNameValue.get(table.getClusteringKey()));
        if (pageIndex == -1) {
            pageIndex = 0;
        }

        Page curPage = Serializer.deserializePage(table.getTableName(), pageIndex);
        Vector<Object> lastTuple = curPage.insertToSorted(table.getColNames(), htblColNameValue);
        table.getMinPerPage().put(pageIndex, (Comparable) curPage.getPage().get(0).get(0));
        Serializer.serializePage(curPage, table.getTableName(), pageIndex);
        int tupleIndex;
        boolean newInsertFlag = false;
        if (lastTuple == null || (lastTuple != null && lastTuple.get(0) != htblColNameValue.get(table.getColNames().get(0)))) {
            tupleIndex = curPage.getTupleIndex((Comparable) htblColNameValue.get(table.getColNames().get(0)));
            table.insertInOctree(deserializedOctrees, curPage.getPage().get(tupleIndex), pageIndex, tupleIndex);
            for (int i = tupleIndex + 1; i < Math.min(curPage.getPage().size(), maxPageSize); i++) {
                table.updatePointerInOctree(deserializedOctrees, curPage.getPage().get(i), pageIndex, i - 1, pageIndex, i);
            }
        }
        if (lastTuple != null && lastTuple.get(0) == htblColNameValue.get(table.getColNames().get(0)) && pageIndex + 1 < fileCount) {
            table.insertInOctree(deserializedOctrees, lastTuple, pageIndex + 1, 0);
            newInsertFlag = true;
        }
        pageIndex++;
        while (lastTuple != null && pageIndex < fileCount) {
            curPage = Serializer.deserializePage(table.getTableName(), pageIndex);

            lastTuple = curPage.insertAtBeginning(lastTuple);

            table.getMinPerPage().put(pageIndex, (Comparable) curPage.getPage().get(0).get(0));
            Serializer.serializePage(curPage, table.getTableName(), pageIndex);

            if (!newInsertFlag) {
                table.updatePointerInOctree(deserializedOctrees, curPage.getPage().get(0), pageIndex - 1, maxPageSize - 1, pageIndex, 0);
            } else {
                newInsertFlag = false;
            }

            for (int i = 1; i < Math.min(curPage.getPage().size(), maxPageSize); i++) {
                table.updatePointerInOctree(deserializedOctrees, curPage.getPage().get(i), pageIndex, i - 1, pageIndex, i);
            }
            pageIndex++;
        }
        if (lastTuple != null) {
            //create new page
            Page page = new Page();
            table.getTable().add(page);
            page.getPage().add(lastTuple);
            table.getMinPerPage().put(pageIndex, (Comparable) lastTuple.get(0));
            Serializer.serializePage(page, table.getTableName(), pageIndex);
            if (lastTuple.get(0) == htblColNameValue.get(table.getColNames().get(0))) {
                table.insertInOctree(deserializedOctrees, lastTuple, pageIndex, 0);
            } else {
                table.updatePointerInOctree(deserializedOctrees, lastTuple, pageIndex - 1, maxPageSize - 1, pageIndex, 0);
            }
        }

        for (Octree octree : deserializedOctrees) {
            Serializer.serializeIndex(octree);
        }

    }

}
