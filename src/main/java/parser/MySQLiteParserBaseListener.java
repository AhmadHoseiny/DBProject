package parser;

import exceptions.DBAppException;
import helper_classes.SQLTerm;
import helper_classes.Serializer;
import table.Table;

import java.lang.reflect.Method;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

public class MySQLiteParserBaseListener extends SQLiteParserBaseListener {

    Iterator res;
    boolean exceptionShouldBeThrown;
    String exceptionMessage;


    public MySQLiteParserBaseListener() {
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
        for (SQLiteParser.Column_defContext parsedCol : ctx.column_def()) {
            String colName = parsedCol.column_name().getText().toLowerCase();
            String colType = getType(parsedCol.type_name().getText().toLowerCase());
            htblColNameType.put(colName, colType);
            htblColNameMin.put(colName, getMin(colType));
            htblColNameMax.put(colName, getMax(colType));
            if (!parsedCol.column_constraint().isEmpty()) {
                String constraint = parsedCol.column_constraint().get(0).getText().toLowerCase();
                if (constraint.equals("primarykey")) {
                    strClusteringKeyColumn = colName;
                } else {
                    this.exceptionShouldBeThrown = true;
                    this.exceptionMessage = "Unsupported Constraint";
                }
            }
        }

        if (!this.exceptionShouldBeThrown) {
            try {
                Class<?> DBApp = Class.forName("DBApp");
                Object dbApp = DBApp.getConstructor().newInstance();
                Method createTable = DBApp.getMethod("createTable", String.class,
                        String.class, Hashtable.class, Hashtable.class, Hashtable.class);
                createTable.invoke(dbApp, strTableName, strClusteringKeyColumn, htblColNameType, htblColNameMin, htblColNameMax);
            } catch (Exception e) {
                setException(e);
            }
        }


    }

    @Override
    public void enterInsert_stmt(SQLiteParser.Insert_stmtContext ctx) {
        String strTableName = ctx.table_name().getText().toLowerCase();
        Hashtable<String, Object> htblColNameValue = new Hashtable<>();

        List<SQLiteParser.Column_nameContext> inpColNames = ctx.column_name();
        List<SQLiteParser.ExprContext> inpColVals = ctx.expr();
        for (int i = 0; i < inpColNames.size(); i++) {
            String inpColName = inpColNames.get(i).getText().toLowerCase();
            String inpColVal = inpColVals.get(i).getText().toLowerCase();
            Object inpColValObj = getObjVal(strTableName, inpColName, inpColVal);
            htblColNameValue.put(inpColName, inpColValObj);
        }
        if (!this.exceptionShouldBeThrown) {
            try {
                Class<?> DBApp = Class.forName("DBApp");
                Object dbApp = DBApp.getConstructor().newInstance();
                Method insertIntoTable = DBApp.getMethod("insertIntoTable", String.class,
                        Hashtable.class);
                insertIntoTable.invoke(dbApp, strTableName, htblColNameValue);
            } catch (Exception e) {
                setException(e);
            }
        }

    }

    @Override
    public void enterUpdate_stmt(SQLiteParser.Update_stmtContext ctx) {
        String strTableName = ctx.qualified_table_name().getText().toLowerCase();
        Hashtable<String, Object> htblColNameValue = new Hashtable<>();
        String strClusteringKeyValue;

        List<SQLiteParser.Column_nameContext> inpColNames = ctx.column_name();
        List<SQLiteParser.ExprContext> exprList = ctx.expr();
        for (int i = 0; i < inpColNames.size(); i++) {
            String inpColName = inpColNames.get(i).getText().toLowerCase();
            String inpColVal = exprList.get(i).getText().toLowerCase();
            Object inpColValObj = getObjVal(strTableName, inpColName, inpColVal);
            htblColNameValue.put(inpColName, inpColValObj);
        }

        int idx = inpColNames.size();
        Vector<SQLiteParser.ExprContext> statements = new Vector<>();
        Vector<String> operators = new Vector<>();
        dfs(exprList.get(idx), statements, operators);

        if (statements.size() != 1)
            setException(new DBAppException("Unsupported Statement..." +
                    "only one condition is allowed in update"));

        Vector<String> colNameVal = new Vector<>();
        Vector<String> operator = new Vector<>();
        parseStatement(statements.get(0), colNameVal, operator);

        String colName = colNameVal.get(0);
        if (!isClusteringKey(strTableName, colName) || !operator.get(0).equals("="))
            setException(new DBAppException("Unsupported Statement..." +
                    "the only condition must be about the clustering key and " +
                    "the operator must be ="));

        strClusteringKeyValue = colNameVal.get(1);

        if (!this.exceptionShouldBeThrown) {
            try {
                Class<?> DBApp = Class.forName("DBApp");
                Object dbApp = DBApp.getConstructor().newInstance();
                Method updateTable = DBApp.getMethod("updateTable", String.class,
                        String.class, Hashtable.class);
                updateTable.invoke(dbApp, strTableName, strClusteringKeyValue, htblColNameValue);
            } catch (Exception e) {
                setException(e);
            }
        }


    }

    @Override
    public void enterDelete_stmt(SQLiteParser.Delete_stmtContext ctx) {
        String strTableName = ctx.qualified_table_name().getText().toLowerCase();
        Hashtable<String, Object> htblColNameValue = new Hashtable<>();

        SQLiteParser.ExprContext expr = ctx.expr();
        if (expr != null) { //otherwise, all records are deleted
            Vector<SQLiteParser.ExprContext> statements = new Vector<>();
            Vector<String> operators = new Vector<>();
            dfs(expr, statements, operators);

            boolean allAnd = true;
            for (int i = 0; i < operators.size(); i++) {
                allAnd &= operators.get(i).equals("and");
            }
            if (!allAnd) {
                setException(new DBAppException("All Operators must be \"AND\" in delete"));
            }

            for (SQLiteParser.ExprContext e : statements) {
                Vector<String> colNameVal = new Vector<>();
                Vector<String> operator = new Vector<>();
                parseStatement(e, colNameVal, operator);
                if (!operator.get(0).equals("="))
                    setException(new DBAppException("All Operators must be \"=\" in delete"));
                String inpColName = colNameVal.get(0).toLowerCase();
                String inpColVal = colNameVal.get(1).toLowerCase();
                Object inpColValObj = getObjVal(strTableName, inpColName, inpColVal);
                htblColNameValue.put(inpColName, inpColValObj);
            }
        }
        if (!this.exceptionShouldBeThrown) {
            try {
                Class<?> DBApp = Class.forName("DBApp");
                Object dbApp = DBApp.getConstructor().newInstance();
                Method deleteFromTable = DBApp.getMethod("deleteFromTable", String.class,
                        Hashtable.class);
                deleteFromTable.invoke(dbApp, strTableName, htblColNameValue);
            } catch (Exception e) {
                setException(e);
            }
        }


    }

    @Override
    public void enterSelect_stmt(SQLiteParser.Select_stmtContext ctx) {

        List<SQLiteParser.Table_or_subqueryContext> tablesList =
                ctx.select_core().get(0).table_or_subquery();
        if (tablesList.size() > 1) {
            setException(new DBAppException("Unsupported Statement..." +
                    "Only one table should be utilized"));
        }

        String strTableName = tablesList.get(0).table_name().getText();

        List<SQLiteParser.ExprContext> exprList = ctx.select_core().get(0).expr();

        Vector<SQLiteParser.ExprContext> statements = new Vector<>();
        Vector<String> operators = new Vector<>();
        dfs(exprList.get(0), statements, operators);

        SQLTerm[] arrSQLTerms = new SQLTerm[statements.size()];
        String[] strarrOperators = new String[operators.size()];

        for (int i = 0; i < statements.size(); i++) {
            SQLiteParser.ExprContext e = statements.get(i);

            Vector<String> colNameVal = new Vector<>();
            Vector<String> operator = new Vector<>();
            parseStatement(e, colNameVal, operator);

            String inpColName = colNameVal.get(0).toLowerCase();
            if (inpColName.contains(".")) { //to handle cases like "T1.name"
                inpColName = inpColName.split("\\.")[1];
            }
            String inpColVal = colNameVal.get(1).toLowerCase();
            Object inpColValObj = getObjVal(strTableName, inpColName, inpColVal);

            SQLTerm sqlTerm = formSQLTerm(strTableName, inpColName,
                    operator.get(0), inpColValObj);
            arrSQLTerms[i] = sqlTerm;
        }
        for (int i = 0; i < operators.size(); i++) {
            strarrOperators[i] = operators.get(i);
        }

        if (!this.exceptionShouldBeThrown) {
            try {
                Class<?> DBApp = Class.forName("DBApp");
                Object dbApp = DBApp.getConstructor().newInstance();
                Method selectFromTable = DBApp.getMethod("selectFromTable", SQLTerm[].class,
                        String[].class);
                this.res = (Iterator) selectFromTable.invoke(dbApp, arrSQLTerms, strarrOperators);
            } catch (Exception e) {
                setException(e);
            }
        }


    }

    @Override
    public void enterCreate_index_stmt(SQLiteParser.Create_index_stmtContext ctx) {
        String strTableName = ctx.table_name().getText().toLowerCase();
        int cntCols = ctx.indexed_column().size();
        if (cntCols != 3)
            setException(new DBAppException("An Octree (the only supported index) must be" +
                    "created on three columns"));
        String strarrColName[] = new String[cntCols];
        for (int i = 0; i < cntCols; i++) {
            SQLiteParser.Indexed_columnContext col = ctx.indexed_column().get(i);
            strarrColName[i] = col.getText().toLowerCase();
        }
        if (!this.exceptionShouldBeThrown) {
            try {
                Class<?> DBApp = Class.forName("DBApp");
                Object dbApp = DBApp.getConstructor().newInstance();
                Method createIndex = DBApp.getMethod("createIndex", String.class,
                        String[].class);
                createIndex.invoke(dbApp, strTableName, strarrColName);
            } catch (Exception e) {
                setException(e);
            }

        }
    }


    //used for parsing "name=Ali AND age>=18 OR salary!=2000"
    public void dfs(SQLiteParser.ExprContext e,
                    Vector<SQLiteParser.ExprContext> statements,
                    Vector<String> operators) {
        if (e.expr().get(0).expr().isEmpty()) {
            statements.add(e);
            return;
        }

        dfs(e.expr().get(0), statements, operators);

        if (e.AND_() != null)
            operators.add("and");
        else if (e.OR_() != null)
            operators.add("or");
        else setException(new DBAppException("Unsupported Logical Operator"));

        dfs(e.expr().get(1), statements, operators);

    }

    //used for parsing "name=Ahmad" or "age>=20"
    public void parseStatement(SQLiteParser.ExprContext e,
                               Vector<String> colNameVal,
                               Vector<String> operator) {
        String colName = e.expr().get(0).getText().toLowerCase();
        String colVal = e.expr().get(1).getText().toLowerCase();
        colNameVal.add(colName);
        colNameVal.add(colVal);
        if (e.ASSIGN() != null)
            operator.add("=");
        else if (e.GT_EQ() != null)
            operator.add(">=");
        else if (e.GT() != null)
            operator.add(">");
        else if (e.LT_EQ() != null)
            operator.add("<=");
        else if (e.LT() != null)
            operator.add("<");
        else if (e.NOT_EQ1() != null || e.NOT_EQ2() != null)
            operator.add("!=");
        else
            setException(new DBAppException("Unsupported Operator"));
    }


    public SQLTerm formSQLTerm(String strTableName, String colName,
                               String operator, Object colVal) {
        SQLTerm sqlTerm = new SQLTerm();
        sqlTerm._strTableName = strTableName;
        sqlTerm._strColumnName = colName;
        sqlTerm._strOperator = operator;
        sqlTerm._objValue = (Comparable) colVal;
        return sqlTerm;
    }

    public void setException(Exception e) {
        this.exceptionShouldBeThrown = true;
        this.exceptionMessage = e.getMessage();
    }

    public String getType(String SQLType) {
        if (SQLType.equals("int")) {
            return "java.lang.Integer";
        } else if (SQLType.contains("decimal") || SQLType.contains("float")) {
            return "java.lang.double";
        } else if (SQLType.contains("varchar")) {
            return "java.lang.String";
        } else if (SQLType.contains("date")) {
            return "java.util.Date";
        } else {
            this.exceptionShouldBeThrown = true;
            this.exceptionMessage = "Unsupported datatype";
            return "";
        }
    }

    public String getMin(String colType) {
        switch (colType) {
            case "java.lang.Integer":
                return Integer.MIN_VALUE + "";
            case "java.lang.double":
                return Double.MIN_VALUE + "";
            case "java.lang.String":
                return "";
            case "java.util.Date":
                return "1970-01-01";
            default:
                this.exceptionShouldBeThrown = true;
                this.exceptionMessage = "Unsupported datatype";
                return "";
        }
    }

    public String getMax(String colType) {
        switch (colType) {
            case "java.lang.Integer":
                return Integer.MAX_VALUE + "";
            case "java.lang.double":
                return Double.MAX_VALUE + "";
            case "java.lang.String":
                return "\u00A0\u00A0\u00A0\u00A0\u00A0\u00A0\u00A0\u00A0";
            case "java.util.Date":
                return "2025-01-01";
            default:
                this.exceptionShouldBeThrown = true;
                this.exceptionMessage = "Unsupported datatype";
                return "";
        }
    }

    public Object getObjVal(String strTableName, String inpColName, String inpColVal) {
        Table t = null;
        try {
            t = Serializer.deserializeTable(strTableName);
        } catch (Exception e) {
            setException(e);
        }
        String colType = t.getColTypes().get(t.getColNames().indexOf(inpColName));
        Object inpColValObj = null;
        switch (colType) {
            case "java.lang.Integer":
                inpColValObj = Integer.parseInt(inpColVal);
                break;
            case "java.lang.double":
            case "java.lang.Double":
                inpColValObj = Double.parseDouble(inpColVal);
                break;
            case "java.lang.String":
                inpColValObj = inpColVal.toLowerCase();
                break;
            case "java.util.Date":
                String pattern = "yyyy-MM-dd";
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
                try {
                    inpColValObj = simpleDateFormat.parse(inpColVal);
                } catch (ParseException e) {
                    setException(e);
                }
                break;
        }
        return inpColValObj;
    }

    public boolean isClusteringKey(String strTableName, String colName) {
        Table t = null;
        try {
            t = Serializer.deserializeTable(strTableName);
        } catch (Exception e) {
            setException(e);
        }
        return colName.equals(t.getColNames().get(0));
    }
}





















