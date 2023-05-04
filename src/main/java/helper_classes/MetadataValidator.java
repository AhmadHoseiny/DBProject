package helper_classes;

import exceptions.DBAppException;

import java.text.SimpleDateFormat;
import java.util.Date;

public class MetadataValidator {
    public static void validate(String colType, String colMin, String colMax) throws DBAppException {
        boolean isValid = true;
        switch (colType) {
            case "java.lang.Integer":
                try {
                    if (Integer.compare(Integer.parseInt(colMin), Integer.parseInt(colMax)) > 0)
                        isValid &= false;
                } catch (Exception e) {
                    isValid &= false;
                }
                break;
            case "java.lang.String":
                if (colMin.compareTo(colMax) > 0)
                    isValid &= false;
                break;
            case "java.lang.Double":
            case "java.lang.double":
                try {
                    if (Double.compare(Double.parseDouble(colMin), Double.parseDouble(colMax)) > 0)
                        isValid &= false;
                } catch (Exception e) {
                    isValid &= false;
                }
                break;
            case "java.util.Date":
                try {
                    String pattern = "yyyy-MM-dd";
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
                    Date dateMin = simpleDateFormat.parse(colMin);
                    Date dateMax = simpleDateFormat.parse(colMax);
                    if (dateMin.compareTo(dateMax) > 0)
                        isValid &= false;
                } catch (Exception e) {
                    isValid &= false;
                }
                break;
            default:
                isValid &= false;
        }

        if (!isValid)
            throw new DBAppException("Invalid min/max values for column type");
    }
}
