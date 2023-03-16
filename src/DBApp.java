import java.io.*;
import java.util.*;

import exceptions.*;
import helper_classes.*;
import tables.*;

public class DBApp {

    public static void main(String[] args) throws IOException, DBAppException, ClassNotFoundException {

        Hashtable htblColNameType = new Hashtable( );
        htblColNameType.put("id", "java.lang.Integer");
        htblColNameType.put("name", "java.lang.String");
        htblColNameType.put("gpa", "java.lang.double");

        Table t = new Table("Student", "id", htblColNameType,
                null, null);

        Hashtable<String, Object> htblColNameValue = new Hashtable( );
        htblColNameValue.put("id", 1);
        htblColNameValue.put("name", new String("Ahmed Noor" ) );
        htblColNameValue.put("gpa", 0.95);
        t.insertTuple(htblColNameValue);

        htblColNameValue = new Hashtable( );
        htblColNameValue.put("id", 3);
        htblColNameValue.put("name", new String("Ahmed Omar" ) );
        htblColNameValue.put("gpa", 1.95);
        t.insertTuple(htblColNameValue);

        htblColNameValue = new Hashtable( );
        htblColNameValue.put("id", 2);
        htblColNameValue.put("name", new String("Ahmed Ali" ) );
        htblColNameValue.put("gpa", 2.95);
        t.insertTuple(htblColNameValue);

        htblColNameValue = new Hashtable( );
        htblColNameValue.put("id", 5);
        htblColNameValue.put("name", new String("Sayed" ) );
        htblColNameValue.put("gpa", 3.95);
        t.insertTuple(htblColNameValue);

        htblColNameValue = new Hashtable( );
        htblColNameValue.put("id", 4);
        htblColNameValue.put("name", new String("Logine" ) );
        htblColNameValue.put("gpa", 4.95);
        t.insertTuple(htblColNameValue);


//        FileOutputStream fileOut =
//                new FileOutputStream("Serialized Files/Page_1.ser");
//        ObjectOutputStream out = new ObjectOutputStream(fileOut);
//        out.writeObject(p);
//        out.close();
//        fileOut.close();
//
//        FileInputStream fileIn = new FileInputStream("Serialized Files/Page_1.ser");
//        ObjectInputStream in = new ObjectInputStream(fileIn);
//        p = (tables.Page) in.readObject();
//        System.out.println(p.page);
//        in.close();
//        fileIn.close();

        for(int i=0 ; i<3 ; i++){
            Page p = Serializer.deserializePage("Student", i);
            System.out.println(p.getPage());
        }

    }

}
