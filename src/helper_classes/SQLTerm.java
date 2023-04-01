package helper_classes;

public class SQLTerm {
    private String _strTableName;
    private String _strColumnName;
    private Operator  _strOperator;

    private Comparable _objValue;

    public SQLTerm(String _strTableName, String _strColumnName, Operator _strOperator, Comparable _objValue) {
        this._strTableName = _strTableName;
        this._strColumnName = _strColumnName;
        this._strOperator = _strOperator;
        this._objValue = _objValue;
    }

    public String get_strTableName() {
        return _strTableName;
    }

    public void set_strTableName(String _strTableName) {
        this._strTableName = _strTableName;
    }

    public String get_strColumnName() {
        return _strColumnName;
    }

    public void set_strColumnName(String _strColumnName) {
        this._strColumnName = _strColumnName;
    }

    public Operator get_strOperator() {
        return _strOperator;
    }

    public void set_strOperator(Operator _strOperator) {
        this._strOperator = _strOperator;
    }

    public Comparable get_objValue() {
        return _objValue;
    }

    public void set_objValue(Comparable _objValue) {
        this._objValue = _objValue;
    }
}
