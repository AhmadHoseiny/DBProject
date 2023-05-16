package parser;

import exceptions.DBAppException;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import java.text.ParseException;
import java.util.Arrays;
import java.util.Iterator;

public class MySQLParser {

    public static Iterator parse(StringBuffer strbufSQL) throws DBAppException {

        CharStream charStream = CharStreams.fromString(strbufSQL.toString());
        SQLiteLexer sqLiteLexer = new SQLiteLexer(charStream);
        CommonTokenStream commonTokenStream = new CommonTokenStream(sqLiteLexer);
        SQLiteParser sqLiteParser = new SQLiteParser(commonTokenStream);
        ParseTree tree = sqLiteParser.parse();

        MySQLiteParserBaseListener mySQLiteParserBaseListener = new MySQLiteParserBaseListener();
        ParseTreeWalker.DEFAULT.walk(mySQLiteParserBaseListener, tree);

        if(mySQLiteParserBaseListener.exceptionShouldBeThrown)
            throw new DBAppException(mySQLiteParserBaseListener.exceptionMessage);

        return mySQLiteParserBaseListener.res;
    }

//    public static void main(String[] args) throws DBAppException, ParseException {
//        StringBuffer createStrBuff = new StringBuffer();
//        createStrBuff.append("Create Table Student(\n" +
//                "\tid int primary key,\n" +
//                "\tname varchar(20),\n" +
//                "\tage decimal(10, 5),\n" +
//                "\tdob DATETIME\n" +
//                ");");
//        StringBuffer insertStrBuff = new StringBuffer();
//        insertStrBuff.append("Insert Into stuDent (ID, name, age, dob)\n" +
//                "Values (5, Ahmad, 22.3, 2002-10-20)");
//
//        StringBuffer updateStrBuff = new StringBuffer();
//        updateStrBuff.append("Update Student\n" +
//                "Set name = omarroka, age = 6.5\n" +
//                "where id = 2");
//
//        StringBuffer deleteStrBuff = new StringBuffer();
//        deleteStrBuff.append("DeletE FrOm StUdent\n");
//
//
//        StringBuffer selectStrBuff = new StringBuffer();
//        selectStrBuff.append("Select *\n" +
//                "From Student S\n" +
//                "where name = ahmad or age>=2");
//
//        StringBuffer createIndexStrBuff = new StringBuffer();
//        createIndexStrBuff.append("Create Index i\n" +
//                "on StuDent (name, age, dob)");
//
//
//        Iterator res = parse(createIndexStrBuff);
////        while(res.hasNext()){
////            System.out.println(res.next());
////        }
//    }
}
