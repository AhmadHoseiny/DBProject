package tables;
import exceptions.DBAppException;
import helper_classes.Operator;
import helper_classes.SQLTerm;
import helper_classes.Serializer;

import java.text.ParseException;
import java.util.*;
import java.io.*;
public class MyIterator implements Iterator{

    private Table table;
    private Page page;
    private int pageIndex;
    private int rowIndex;

    private SQLTerm[] arrSQLTerms;
    private String[] strarrOperators;

    private boolean nextCalled;
    private Vector<Object> nextTuple;

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

    public MyIterator(SQLTerm[] arrSQLTerms, String[] strarrOperators)
            throws DBAppException, IOException, ClassNotFoundException, ParseException {

        //checking validity of input
        boolean sameTable = true;
        for(int i=0 ; i<arrSQLTerms.length-1 ; i++)
            if(!arrSQLTerms[i]._strTableName.equals(arrSQLTerms[i+1]._strTableName))
                sameTable = false;
        if(!sameTable)
            throw new DBAppException("All terms must be from the same table");
        if(strarrOperators.length != arrSQLTerms.length-1)
            throw new DBAppException("Number of operators must be one less than number of terms");
        if(arrSQLTerms.length == 0)
            throw new DBAppException("Nothing to iterate over");

        //initializing iterator
        this.arrSQLTerms = arrSQLTerms;
        this.strarrOperators = strarrOperators;
        String strTableName = arrSQLTerms[0]._strTableName;
        this.table = Serializer.deserializeTable(strTableName);
        this.pageIndex = 0;
        this.rowIndex = -1;
        this.page = Serializer.deserializePage(table.getTableName(), pageIndex);;

        this.nextCalled = false;
        this.nextTuple = null;
    }

    public boolean existsInResultSet(Vector<Object> tuple) throws DBAppException {
        Vector<String> colNames = table.getColNames();
        HashMap<String, Integer> revColName = new HashMap<>();
        for(int i=0 ; i<colNames.size() ; i++)
            revColName.put(colNames.get(i), i);

        boolean exists = true ;
        for(int i=0 ; i<arrSQLTerms.length ; i++){
            String curColName = arrSQLTerms[i]._strColumnName;
            int curColIndex = revColName.get(curColName);
            Comparable curColVal = (Comparable) tuple.get(curColIndex);
            String curOp = arrSQLTerms[i]._strOperator;
            Comparable curVal = (Comparable) arrSQLTerms[i]._objValue;
            Operator op = new Operator(curOp);
            boolean curExists = op.compare(curColVal, curVal);
            if(i==0){
                exists = curExists;
            }
            else{
                if(strarrOperators[i-1].equals("AND")){
                    exists &= curExists;
                }
                else if(strarrOperators[i-1].equals("OR")){
                    exists |= curExists;
                }
                else{
                    exists ^= curExists;
                }
            }
        }
        return exists;
    }


    private void doNextJob() throws IOException, ClassNotFoundException, DBAppException {
        nextCalled = true;
        if(this.page == null){
            this.page = Serializer.deserializePage(table.getTableName(), pageIndex);
        }
        while(true){
            rowIndex++;
            if(rowIndex == this.getPage().getPage().size()){
                pageIndex++;
                if(pageIndex == table.getMinPerPage().size()){
                    this.nextTuple = null;
                    break;
                }
                rowIndex = 0;
                this.page = Serializer.deserializePage(table.getTableName(), pageIndex);
            }
            Vector<Object> curTuple = this.page.getPage().get(rowIndex);
            if(existsInResultSet(curTuple)){
                this.nextTuple = curTuple;
                break;
            }
        }

    }
    @Override
    public boolean hasNext() throws NoSuchElementException {
        if(nextCalled)
            return nextTuple==null? false:true ;
        try{
            doNextJob();
        }
        catch (Exception e){
            throw new NoSuchElementException(e.getMessage());
//            System.out.println(e.getMessage());
        }
        if(nextTuple == null)
            return false;
        return true;
    }

    @Override
    public Object next() throws NoSuchElementException{
        if(!nextCalled)
            try{
                doNextJob();
            }
            catch (Exception e){
                throw new NoSuchElementException(e.getMessage());
//                System.out.println(e.getMessage());
            }
        nextCalled = false;
        return nextTuple;
    }

}
