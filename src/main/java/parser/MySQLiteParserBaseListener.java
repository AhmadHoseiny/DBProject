package parser;

import exceptions.DBAppException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Hashtable;
import java.util.Iterator;

public class MySQLiteParserBaseListener extends SQLiteParserBaseListener{

    Iterator res;
    boolean exceptionShouldBeThrown;
    String exceptionMessage;
    public MySQLiteParserBaseListener(){
        super();
        this.res = null;
        this.exceptionShouldBeThrown = false;
        this.exceptionMessage = "";
    }

    @Override
    public void enterCreate_table_stmt(SQLiteParser.Create_table_stmtContext ctx) {
        String strTableName = ctx.table_name().getText().toLowerCase();
        String strClusteringKeyColumn = "";
        Hashtable<String, String> htblColNameType = new Hashtable<>();
        Hashtable<String, String> htblColNameMin = new Hashtable<>();
        Hashtable<String, String> htblColNameMax = new Hashtable<>();

        for(SQLiteParser.Column_defContext parsedCol : ctx.column_def()){
            String colName = parsedCol.column_name().getText().toLowerCase();
            String colType = getType(parsedCol.type_name().getText().toLowerCase());
            htblColNameType.put(colName, colType);
            htblColNameMin.put(colName, getMin(colType));
            htblColNameMax.put(colName, getMax(colType));
            if(!parsedCol.column_constraint().isEmpty()){
                String constraint = parsedCol.column_constraint().get(0).getText().toLowerCase();
                if(constraint.equals("primarykey")){
                    strClusteringKeyColumn = colName;
                }
                else{
                    this.exceptionShouldBeThrown = true;
                    this.exceptionMessage = "Unsupported Constraint";
                }
            }
        }

        try {
            Class<?> DBApp = Class.forName("DBApp");
            Object dbApp = DBApp.getConstructor().newInstance();
            Method createTable = DBApp.getMethod("createTable", String.class, String.class, Hashtable.class, Hashtable.class, Hashtable.class);
            createTable.invoke(dbApp, strTableName, strClusteringKeyColumn, htblColNameType, htblColNameMin, htblColNameMax);
        } catch (Exception e) {
            this.exceptionShouldBeThrown = true;
            this.exceptionMessage = "Unexpected Error";
        }
    }









    public String getType(String SQLType){
        if(SQLType.equals("int")){
            return "java.lang.Integer";
        } else if (SQLType.contains("decimal") || SQLType.contains("float")) {
            return "java.lang.double";
        } else if (SQLType.contains("varchar")) {
            return "java.lang.String";
        } else if (SQLType.contains("date")) {
            return "java.util.Date";
        }
        else{
            this.exceptionShouldBeThrown = true;
            this.exceptionMessage = "Unsupported datatype";
            return "";
        }
    }

    public String getMin(String colType){
        switch(colType){
            case "java.lang.Integer": return Integer.MIN_VALUE +"";
            case "java.lang.double": return Double.MIN_VALUE +"";
            case "java.lang.String": return "";
            case "java.util.Date": return "1970-01-01";
            default: this.exceptionShouldBeThrown = true;
                     this.exceptionMessage = "Unsupported datatype";
                     return "";
        }
    }
    public String getMax(String colType){
        switch(colType){
            case "java.lang.Integer": return Integer.MAX_VALUE +"";
            case "java.lang.double": return Double.MAX_VALUE +"";
            case "java.lang.String": return "\u00A0\u00A0\u00A0\u00A0\u00A0\u00A0\u00A0\u00A0";
            case "java.util.Date": return "2025-01-01";
            default: this.exceptionShouldBeThrown = true;
                this.exceptionMessage = "Unsupported datatype";
                return "";
        }
    }

}





















