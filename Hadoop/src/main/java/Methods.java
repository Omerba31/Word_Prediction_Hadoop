public class Methods {

    public static String getWord_key(String keyString, int index) {
        return keyString.split(" ")[index];
    }

    public static String getWord_value(String valueString, int index) {
        return valueString.split("\t")[index];
    }

    public static int getKeyLength(String keyString) {
        return keyString.split(" ").length;
    }

    public static int getValueLength(String valueString) {
        return valueString.split("\t").length;
    }


}
