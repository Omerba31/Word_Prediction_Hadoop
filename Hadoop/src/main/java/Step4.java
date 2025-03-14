import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.fs.FileSystem;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Comparator;

public class Step4 {
    public static class Step4_Mapper extends Mapper<LongWritable, Text, Text, Text> {
        private long c0;

        protected void setup(Context context) throws IOException, InterruptedException {
            c0 = context.getConfiguration().getLong("c0_value", 1L);
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] fields = value.toString().split("\t");
            if (fields.length < 7) return;

            String ngram = fields[0];
            String firstWord = ngram.split(" ", 2)[0];

            double[] numbersOfValues = new double[6];
            for (int i = 0; i < 6; i++) {
                String[] currValue = fields[i + 1].split(":");
                if (currValue.length < 2 || (!currValue[0].equals(firstWord) && "null".equals(currValue[1]))) return;
                numbersOfValues[i] = Double.parseDouble(currValue[1]);
            }

            double N1 = numbersOfValues[0];
            double N2 = numbersOfValues[2];
            double N3 = numbersOfValues[5];
            double C1 = numbersOfValues[1];
            double C2 = numbersOfValues[4];

            double K2 = ((Math.log(N2 + 1) / Math.log(2)) + 1) / ((Math.log(N2 + 1) / Math.log(2)) + 2);
            double K3 = ((Math.log(N3 + 1) / Math.log(2)) + 1) / ((Math.log(N3 + 1) / Math.log(2)) + 2);

            double probabilty = K3 * (N3 / C2) + (1 - K3) * K2 * (N2 / C1) + (1 - K3) * (1 - K2) * ((double) N1 / c0);
            String prob = String.format("%.5f", probabilty);

            Text newKey = new Text(ngram + " " + prob);
            Text newValue = new Text("");

            context.write(newKey, newValue);
        }
    }


    public static class Step4_Reducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Text value = values.iterator().next();
            context.write(key, value);
        }
    }

    public static class Step4_Partitioner extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String[] parts = key.toString().split(" ", 3);
            return Math.abs((parts[0] + parts[1]).hashCode()) % numPartitions;
        }
    }

    public static class Step4_Comparator extends WritableComparator {
        protected Step4_Comparator() {
            super(Text.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            String[] ngram1 = ((Text) w1).toString().split(" ");
            String[] ngram2 = ((Text) w2).toString().split(" ");

            int cmp = Comparator.comparing((String[] arr) -> arr[0])
                    .thenComparing(arr -> arr[1])
                    .compare(ngram1, ngram2);

            if (cmp != 0) return cmp;

            double prob1 = Double.parseDouble(ngram1[ngram1.length - 1]);
            double prob2 = Double.parseDouble(ngram2[ngram2.length - 1]);
            return Double.compare(prob2, prob1);
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 4 started!");

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("s3://" + Config.BUCKET_NAME), conf);
        FSDataInputStream in = fs.open(Config.PATH_C0);
        BufferedReader br = new BufferedReader(new InputStreamReader(in));

        String line;
        long C0 = 0;

        while ((line = br.readLine()) != null) {
            if (line.contains("C0 Value:")) {
                String[] parts = line.split(":");
                C0 = Long.parseLong(parts[1].trim());
            }
        }

        br.close();
        in.close();

        Job job = Job.getInstance(conf, "Step 4 - Processing with c0");

        job.setJarByClass(Step4.class);
        job.setMapperClass(Step4_Mapper.class);
        job.setReducerClass(Step4_Reducer.class);
        job.setPartitionerClass(Step4_Partitioner.class);
        job.setSortComparatorClass(Step4_Comparator.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.getConfiguration().setLong("c0_value", C0);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job, Config.OUTPUT_STEP_3);
        TextOutputFormat.setOutputPath(job, Config.OUTPUT_STEP_4);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}