import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Map;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

public class Step1 {

    public static class Step1_Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text word = new Text();
        public static final Set<String> STOP_WORDS = new HashSet<>(Arrays.asList(
                "״", "׳", "של", "רב", "פי", "עם", "עליו", "עליהם", "על", "עד", "מן", "מכל", "מי", "מהם", "מה", "מ",
                "למה", "לכל", "לי", "לו", "להיות", "לה", "לא", "כן", "כמה", "כלי", "כל", "כי", "יש", "ימים", "יותר",
                "יד", "י", "זה", "ז", "ועל", "ומי", "ולא", "וכן", "וכל", "והיא", "והוא", "ואם", "ו", "הרבה", "הנה",
                "היו", "היה", "היא", "הזה", "הוא", "דבר", "ד", "ג", "בני", "בכל", "בו", "בה", "בא", "את", "אשר",
                "אם", "אלה", "אל", "אך", "איש", "אין", "אחת", "אחר", "אחד", "אז", "אותו", "־", "^", "?", ";", ":",
                "1", ".", "-", "*", "\"", "!", "שלשה", "בעל", "פני", ")", "גדול", "שם", "עלי", "עולם", "מקום",
                "לעולם", "לנו", "להם", "ישראל", "יודע", "זאת", "השמים", "הזאת", "הדברים", "הדבר", "הבית",
                "האמת", "דברי", "במקום", "בהם", "אמרו", "אינם", "אחרי", "אותם", "אדם", "(", "חלק", "שני",
                "שכל", "שאר", "ש", "ר", "פעמים", "נעשה", "ן", "ממנו", "מלא", "מזה", "ם", "לפי", "ל", "כמו",
                "כבר", "כ", "זו", "ומה", "ולכל", "ובין", "ואין", "הן", "היתה", "הא", "ה", "בל", "בין", "בזה",
                "ב", "אף", "אי", "אותה", "או", "אבל", "א"
        ));

        // ngram format: ngram \t year \t match_count \t page_count \t volume_count \n
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\t");

            if (fields.length == 5) {
                String ngram = fields[0];
                String[] words = ngram.split(" ");
                for (String word : words) {
                    if (STOP_WORDS.contains(word) || !word.matches("^[\\p{IsHebrew}]+$"))
                        return;
                }
                IntWritable count = new IntWritable(Integer.parseInt(fields[2]));
                Text ngramText = new Text(ngram);

                context.write(ngramText, count);
            }
        }

    }

    public static class Step1_Reducer extends Reducer<Text, IntWritable, Text, Text> {
        private HashMap<String, Integer> current_nGrams;
        private String curr_1st;

        private enum Counters {
            C0
        }

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            String keyString = key.toString().trim();
            String local_1st = Methods.getWord_key(keyString, 0);

            if (curr_1st == null || !curr_1st.equals(local_1st)) {
                if (current_nGrams != null) {
                    for (Map.Entry<String, Integer> entry : current_nGrams.entrySet()) {
                        String ngram = entry.getKey();

                        String w1 = Methods.getWord_key(ngram, 0);
                        String sum = entry.getValue().toString();

                        Text newKey = new Text(ngram), newValue;

                        Integer w1_count, w1w2_count;
                        String w1Count, w1w2Count;

                        switch (Methods.getKeyLength(ngram)) {
                            case 1:
                                // "w1" -> "w1:sum"
                                newValue = new Text(ngram + ":" + sum);
                                context.write(newKey, newValue);

                                break;

                            case 2:
                                // "w1 w2" -> "w1:w1Count \t w1 w2:sum"
                                w1_count = current_nGrams.get(w1);
                                w1Count = w1_count != null ? w1_count.toString() : "null";

                                newValue = new Text(w1 + ":" + w1Count + "\t" + ngram + ":" + sum);
                                context.write(newKey, newValue);

                                break;

                            case 3:
                                // "w1 w2 w3" -> "w1:w1Count \t w1 w2:w1w2Count \t w1 w2 w3:sum"
                                String w2 = Methods.getWord_key(ngram, 1);
                                w1_count = current_nGrams.get(w1);
                                w1Count = w1_count != null ? w1_count.toString() : "null";

                                w1w2_count = current_nGrams.get(w1 + " " + w2);
                                w1w2Count = w1w2_count != null ? w1w2_count.toString() : "null";

                                newValue = new Text(w1 + ":" + w1Count + "\t" + w1 + " " + w2 + ":" + w1w2Count + "\t" + ngram + ":" + sum);
                                context.write(newKey, newValue);

                                break;
                        }
                    }
                }
                curr_1st = local_1st;
                current_nGrams = new HashMap<>();
            }

            int sum = 0;
            for (IntWritable value : values) sum += value.get();
            context.getCounter(Counters.C0).increment(sum);
            current_nGrams.put(keyString, sum);
        }

        //        public static void combine(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
//            int sum = 0;
//            for (IntWritable value : values) {
//                sum += value.get();
//            }
//            context.write(key, new Text(String.valueOf(sum)));
//        }

    }

    public static class Step1_Partitioner extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            return Math.abs(Methods.getWord_key(key.toString(), 0).hashCode() % numPartitions);
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 1 started!");
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Step1 - Word Counting");
        job.setJarByClass(Step1.class);
        job.setMapperClass(Step1_Mapper.class);
        job.setPartitionerClass(Step1_Partitioner.class);
        job.setReducerClass(Step1_Reducer.class);
        // job.setCombinerClass(Step1_Reducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        SequenceFileInputFormat.addInputPath(job, Config.PATH_1_GRAM);
        SequenceFileInputFormat.addInputPath(job, Config.PATH_2_GRAM);
        SequenceFileInputFormat.addInputPath(job, Config.PATH_3_GRAM);
        TextOutputFormat.setOutputPath(job, Config.OUTPUT_STEP_1);

        boolean success = job.waitForCompletion(true);

        if (success) {
            FileSystem fs = FileSystem.get(new URI("s3://" + Config.BUCKET_NAME), conf);

            long c0Value = job.getCounters().findCounter(Step1_Reducer.Counters.C0).getValue();
            FSDataOutputStream out = fs.create(Config.PATH_C0);
            out.writeBytes("C0 Value: " + c0Value + "\n");
            out.close();
        }
        System.exit(success ? 0 : 1);
    }

}