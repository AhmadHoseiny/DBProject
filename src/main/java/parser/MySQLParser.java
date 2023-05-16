package parser;

import exceptions.DBAppException;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

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

        if (mySQLiteParserBaseListener.exceptionShouldBeThrown)
            throw new DBAppException(mySQLiteParserBaseListener.exceptionMessage);

        return mySQLiteParserBaseListener.res;
    }

}
