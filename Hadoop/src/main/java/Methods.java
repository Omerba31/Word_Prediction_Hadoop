import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.AbstractMap;

public class Methods {

    //General methods for the program
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

    //Step2:
    public static AbstractMap.SimpleEntry<Text, Text> processSingleWordKey(Text key, Iterable<Text> values) throws IOException, InterruptedException {
        StringBuilder builder = new StringBuilder();
        Integer numOfValues = 0;

        for (Text value : values) {
            String valueStr = value.toString();
            builder.append(valueStr);
            numOfValues++;
        }

        Text newValue = new Text(builder.toString());
        return new AbstractMap.SimpleEntry<>(key, newValue);
    }

    public static LinkedList<AbstractMap.SimpleEntry<Text, Text>> processTwoWordsKey(Text key, Iterable<Text> values) throws IOException, InterruptedException {
        String value_2gram = "null";
        List<Text> newValues = new LinkedList<>();
        int numOfValues = 0;

        for (Text value : values) {
            String valueStr = value.toString();
            if (Methods.getValueLength(valueStr) == 2) {
                value_2gram = valueStr;
                numOfValues++;
            } else
                newValues.add(value);
        }

        LinkedList <AbstractMap.SimpleEntry<Text, Text>> result = new LinkedList<>();
        for (Text value : newValues) {
            Text newValue = new Text(value_2gram + "\t" + value.toString());
            Text newKey = new Text(Methods.getWord_key(key.toString(), 1));
            result.add(new AbstractMap.SimpleEntry<>(newKey, newValue));
        }

        return result;
    }
}
