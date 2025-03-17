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

import java.util.LinkedList;
import java.util.AbstractMap;
import java.io.IOException;
import java.util.List;
import java.util.Arrays;

public class Step2 {

    public static class Step2_Mapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\t");
            Text newKey, newValue;

            String ngram = fields[0];
            String[] ngramWords = ngram.split(" ");
            String valuesInString = String.join("\t", Arrays.copyOfRange(fields, 1, fields.length));

            if (ngramWords.length == 3) {
                newKey = new Text(ngramWords[1] + " " + ngramWords[2]);
                newValue = new Text(valuesInString);

            } else {
                newKey = new Text(ngram);
                newValue = new Text(valuesInString);
            }
            context.write(newKey, newValue);
        }

    }

    public static class Step2_Reducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            switch (Methods.getKeyLength(key.toString())) {
                case 1:
                    AbstractMap.SimpleEntry<Text, Text> KV = Methods.processSingleWordKey(key, values);
                    context.write(KV.getKey(), KV.getValue());
                    break;

                case 2:
                    LinkedList<AbstractMap.SimpleEntry<Text, Text>> KVs = Methods.processTwoWordsKey(key, values);
                    for (AbstractMap.SimpleEntry<Text, Text> kv : KVs)
                        context.write(kv.getKey(), kv.getValue());

                    break;

                default:
                    System.err.println("[ERROR] Invalid key length: " + key.toString());
            }
        }
    }

    public static class Step2_Partitioner extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return Math.abs(Methods.getWord_key(key.toString(), 0).hashCode() % numPartitions);
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 2 started!");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step2 - First Join");

        job.setJarByClass(Step2.class);
        job.setMapperClass(Step2_Mapper.class);
        job.setPartitionerClass(Step2_Partitioner.class);
        job.setReducerClass(Step2_Reducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job, Config.OUTPUT_STEP_1);
        TextOutputFormat.setOutputPath(job, Config.OUTPUT_STEP_2);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}