package helper_classes;

import exceptions.DBAppException;

public class Operator {

    String strOperator;

    public Operator(String strOperator) {
        this.strOperator = strOperator;
    }

    public boolean compare(Comparable val1, Comparable val2) throws DBAppException {
        switch (strOperator) {
            case "=":
                return val1.compareTo(val2) == 0;
            case ">":
                return val1.compareTo(val2) > 0;
            case ">=":
                return val1.compareTo(val2) >= 0;
            case "<":
                return val1.compareTo(val2) < 0;
            case "<=":
                return val1.compareTo(val2) <= 0;
            case "!=":
                return val1.compareTo(val2) != 0;
            default:
                throw new DBAppException("Invalid operator");
        }
    }
}
