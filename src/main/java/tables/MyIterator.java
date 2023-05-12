package tables;

import exceptions.DBAppException;
import helper_classes.GenericComparator;
import helper_classes.Operator;
import helper_classes.SQLTerm;
import helper_classes.Serializer;
import index.Octree;

import java.io.IOException;
import java.text.ParseException;
import java.util.*;

public class MyIterator implements Iterator {

    private Table table;
    private Page page;
    private int pageIndex;
    private int rowIndex;

    private SQLTerm[] arrSQLTerms;
    private String[] strarrOperators;

    private boolean nextCalled;
    private Vector<Object> nextTuple;

    private boolean usingIndex;
    private String indexName;
    private Vector<Vector<Object>> resultSet;
    private int resultSetPointer;

    public MyIterator(SQLTerm[] arrSQLTerms, String[] strarrOperators)
            throws DBAppException, IOException, ClassNotFoundException, ParseException {

        //checking validity of input
        boolean sameTable = true;
        for (int i = 0; i < arrSQLTerms.length - 1; i++)
            if (!arrSQLTerms[i]._strTableName.equals(arrSQLTerms[i + 1]._strTableName))
                sameTable = false;
        if (!sameTable)
            throw new DBAppException("All terms must be from the same table");
        if (strarrOperators.length != arrSQLTerms.length - 1)
            throw new DBAppException("Number of operators must be one less than number of terms");
        if (arrSQLTerms.length == 0)
            throw new DBAppException("Nothing to iterate over");


        //initializing iterator
        this.arrSQLTerms = arrSQLTerms;
        this.strarrOperators = strarrOperators;
        String strTableName = arrSQLTerms[0]._strTableName;
        this.table = Serializer.deserializeTable(strTableName);
        this.pageIndex = 0;
        this.rowIndex = -1;
        this.page = Serializer.deserializePage(table.getTableName(), pageIndex);

        this.nextCalled = false;
        this.nextTuple = null;

        //to determine if we are using the index or not
        this.compUsingIndex();
        System.out.println("Using index(Constructor): " + usingIndex);
        if (this.usingIndex) {
            this.resultSet = new Vector<>();
            this.resultSetPointer = 0;
            this.compResultSet();
        }
    }

    public void compUsingIndex() {
        usingIndex = checkANDConditions();
//        System.out.println("Using index: " + usingIndex);
        if (usingIndex) {
            indexName = checkIfColumnsInIndex(arrSQLTerms);
            if (indexName == null)
                usingIndex = false;
        }
    }

    public void compResultSet() throws DBAppException, IOException, ParseException, ClassNotFoundException {

        Octree octree = Serializer.deserializeIndex(table, indexName);
        Vector<Comparable> objValues = new Vector<>();
        Vector<String> operators = new Vector<>();

        for (int i = 0; i < 3; i++) {
            String currColName = octree.getStrarrColName()[i];
            Comparable currObjValue = null;
            String currOperator = null;
            for (SQLTerm term : arrSQLTerms) {
                if (term._strOperator.equals("!="))
                    continue;
                if (term._strColumnName.equals(currColName)) {
                    if (currOperator == null) {
                        currObjValue = term._objValue;
                        currOperator = term._strOperator;
                    } else if (!currOperator.equals("=") && term._strOperator.equals("=")) {
                        currObjValue = term._objValue;
                        currOperator = term._strOperator;
                    } else if (currOperator.equals("=")) {
                        break;
                    }
                }
            }
            objValues.add(currObjValue);
            operators.add(currOperator);
        }
        //pageIndex --> LinkedList of rowIndices
        TreeMap<Integer, LinkedList<Integer>> resPointers = octree.searchForSelect(objValues, operators);
        for (int pageIndex : resPointers.keySet()) {
            LinkedList<Integer> rowIndices = resPointers.get(pageIndex);
            Page p = Serializer.deserializePage(table.getTableName(), pageIndex);
            for (int rowIndex : rowIndices) {
                Vector<Object> tuple = p.getPage().get(rowIndex);
                if (existsInResultSet(tuple)) {
                    resultSet.add(tuple);
                }
            }
        }

        Collections.sort(resultSet, (a, b) -> GenericComparator.compare((Comparable) a.get(0), (Comparable) b.get(0)));

//        resultSet = octree.searchForSelect(objValues, operators);


    }

    public boolean checkANDConditions() {
        for (String operator : this.strarrOperators) {
            if (!operator.equals("and"))
                return false;
        }
        return true;
    }

    public String checkIfColumnsInIndex(SQLTerm[] arrSQLTerms) {
        for (String index : table.getIndexNames()) {
            if (index == null)
                continue;
            String[] columnNames = index.split("(?=\\p{Upper})");
            String colName1 = columnNames[0].toLowerCase();
            String colName2 = columnNames[1].toLowerCase();
            String colName3 = columnNames[2].toLowerCase();
//            System.out.println(colName1 + " " + colName2 + " " + colName3);
            HashSet<String> colNames = new HashSet<>();
            int count = 0;
            for (SQLTerm term : arrSQLTerms) {
                if (term._strOperator.equals("!="))
                    continue;
                if (term._strColumnName.equals(colName1) && !colNames.contains(colName1)) {
                    colNames.add(term._strColumnName);
                    count++;
//                    System.out.println("Count: " + count + " " + colName1);
                } else if (term._strColumnName.equals(colName2) && !colNames.contains(colName2)) {
                    colNames.add(term._strColumnName);
                    count++;
//                    System.out.println("Count: " + count + " " + colName2);
                } else if (term._strColumnName.equals(colName3) && !colNames.contains(colName3)) {
                    colNames.add(term._strColumnName);
                    count++;
//                    System.out.println("Count: " + count + " " + colName3);
                }
            }
//            System.out.println("Count: " + count);
            if (count == 3)
                return index;
        }
        return null;
    }

    public Table getTable() {
        return table;
    }

    public Page getPage() {
        return page;
    }

    public int getPageIndex() {
        return pageIndex;
    }

    public void setPageIndex(int pageIndex) {
        this.pageIndex = pageIndex;
    }

    public int getRowIndex() {
        return rowIndex;
    }

    public void setRowIndex(int rowIndex) {
        this.rowIndex = rowIndex;
    }

    public SQLTerm[] getArrSQLTerms() {
        return arrSQLTerms;
    }

    public String[] getStrarrOperators() {
        return strarrOperators;
    }

    public boolean existsInResultSet(Vector<Object> tuple) throws DBAppException {
        Vector<String> colNames = table.getColNames();
        HashMap<String, Integer> revColName = new HashMap<>();
        for (int i = 0; i < colNames.size(); i++)
            revColName.put(colNames.get(i), i);

        boolean exists = true;
        for (int i = 0; i < arrSQLTerms.length; i++) {
            String curColName = arrSQLTerms[i]._strColumnName;
            int curColIndex = revColName.get(curColName);
            Comparable curColVal = (Comparable) tuple.get(curColIndex);
            String curOp = arrSQLTerms[i]._strOperator;
            Comparable curVal = (Comparable) arrSQLTerms[i]._objValue;
            Operator op = new Operator(curOp);
            boolean curExists = op.compare(curColVal, curVal);
            if (i == 0) {
                exists = curExists;
            } else {
                if (strarrOperators[i - 1].equals("and")) {
                    exists &= curExists;
                } else if (strarrOperators[i - 1].equals("or")) {
                    exists |= curExists;
                } else {
                    exists ^= curExists;
                }
            }
        }
        return exists;
    }


    private void doNextJob() throws IOException, ClassNotFoundException, DBAppException {
        nextCalled = true;
        if (this.page == null) {
            this.page = Serializer.deserializePage(table.getTableName(), pageIndex);
        }
        while (true) {
            rowIndex++;
            if (rowIndex == this.getPage().getPage().size()) {
                pageIndex++;
                if (pageIndex == table.getMinPerPage().size()) {
                    this.nextTuple = null;
                    break;
                }
                rowIndex = 0;
                this.page = Serializer.deserializePage(table.getTableName(), pageIndex);
            }
            Vector<Object> curTuple = this.page.getPage().get(rowIndex);
            if (existsInResultSet(curTuple)) {
                this.nextTuple = curTuple;
                break;
            }
        }

    }

    @Override
    public boolean hasNext() throws NoSuchElementException {
        if (this.usingIndex) {
            if (resultSetPointer == resultSet.size())
                return false;
        } else {
            if (nextCalled)
                return nextTuple == null ? false : true;
            try {
                doNextJob();
            } catch (Exception e) {
                throw new NoSuchElementException(e.getMessage());
//            System.out.println(e.getMessage());
            }
            if (nextTuple == null)
                return false;
        }
        return true;
    }

    @Override
    public Object next() throws NoSuchElementException {
        if (this.usingIndex) {
            if (resultSetPointer == resultSet.size())
                throw new NoSuchElementException("No more elements");
            return resultSet.get(resultSetPointer++);
        } else {
            if (!nextCalled)
                try {
                    doNextJob();
                } catch (Exception e) {
                    throw new NoSuchElementException(e.getMessage());
//                System.out.println(e.getMessage());
                }
            nextCalled = false;
            return nextTuple;
        }
    }

}
