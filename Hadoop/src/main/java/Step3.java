import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Arrays;

public class Step3 {

    public static class Step3_Mapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] fields = value.toString().split("\t");
            String ngram = fields[0];
            String[] ngramWords = ngram.split(" ");

            if (ngramWords.length != 1) return;
            String valuesInString = String.join("\t", Arrays.copyOfRange(fields, 1, fields.length));

            context.write(new Text(ngram), new Text(valuesInString));
        }
    }


    public static class Step3_Reducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            if (Methods.getKeyLength(key.toString()) != 1) return;

            String value_1gram = "null";
            List<Text> notOriginalValues = new LinkedList<>();

            for (Text value : values) {
                if (Methods.getValueLength(value.toString()) == 1)
                    value_1gram = value.toString();
                else notOriginalValues.add(value);
            }

            for (Text value : notOriginalValues) {
                String valueStr = value.toString();
                if (Methods.getValueLength(valueStr) < 5) return;

                String value_3gram_4 = Methods.getWord_value(valueStr, 4);
                String value_3gram = value_3gram_4.substring(0, value_3gram_4.lastIndexOf(":"));
                Text newKey = new Text(value_3gram);
                Text newValue = new Text(value_1gram + "\t" + valueStr);

                context.write(newKey, newValue);
            }
        }
    }

    public static class Step3_Partitioner extends Partitioner<Text, Text> { //Get partition wasnt done today
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return Math.abs(Methods.getWord_key(key.toString(), 0).hashCode() % numPartitions);
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 3 started!");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step3 - Second Join");
        job.setJarByClass(Step3.class);

        job.setMapperClass(Step3_Mapper.class);
        job.setReducerClass(Step3_Reducer.class);
        job.setPartitionerClass(Step3_Partitioner.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job, Config.OUTPUT_STEP_2);
        TextOutputFormat.setOutputPath(job, Config.OUTPUT_STEP_3);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}