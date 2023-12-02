import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ARDriver {

    public static class ARMapper extends Mapper<Object, Text, Text, FloatWritable> {
        private Text genre = new Text();
        private FloatWritable rating = new FloatWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Assuming the input format is: genre \t rating
            StringTokenizer itr = new StringTokenizer(value.toString(), "\t");
            if (itr.countTokens() == 2) {
                genre.set(itr.nextToken());
                try {
                    rating.set(Float.parseFloat(itr.nextToken()));
                    context.write(genre, rating);
                } catch (NumberFormatException e) {
                    // Handle parsing errors
                }
            }
        }
    }

    public static class ARReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        private FloatWritable result = new FloatWritable();

        public void reduce(Text key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {
            float sum = 0;
            int count = 0;
            for (FloatWritable val : values) {
                sum += val.get();
                count++;
            }
            if (count > 0) {
                result.set(sum / count);
                context.write(key, result);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "average rating per genre");
        job.setJarByClass(ARDriver.class);
        job.setMapperClass(ARMapper.class);
        job.setCombinerClass(ARReducer.class);
        job.setReducerClass(ARReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}