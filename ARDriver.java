import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ARDriver {

    public static class ARRatingsMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        private Text genre = new Text();
        private DoubleWritable rating = new DoubleWritable();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Parse the input JSON data and extract genre and rating information
            // Use a JSON parsing library for a more robust implementation
            String[] fields = value.toString().split("\"genre\": \"");
            if (fields.length < 2) {
                return;
            }

            String[] genreField = fields[1].split("\"");
            if (genreField.length < 2) {
                return;
            }

            String genreStr = genreField[0].trim();
            String[] imdbRatingField = fields[1].split("\"imdb_rating\": \"");
            if (imdbRatingField.length < 2) {
                return;
            }

            String[] ratingField = imdbRatingField[1].split("\"");
            if (ratingField.length < 2) {
                return;
            }

            try {
                genre.set(genreStr);
                rating.set(Double.parseDouble(ratingField[0]));
                context.write(genre, rating);
            } catch (NumberFormatException e) {
                // Ignore invalid ratings
            }
        }
    }

    public static class ARRatingsReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private DoubleWritable result = new DoubleWritable();

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;

            for (DoubleWritable value : values) {
                sum += value.get();
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
        Job job = Job.getInstance(conf, "Average Ratings by Genre");

        job.setJarByClass(ARDriver.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(ARRatingsMapper.class);
        job.setReducerClass(ARRatingsReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
