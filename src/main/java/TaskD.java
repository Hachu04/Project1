import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * use a single map reduce job
 * the idea is pretty similar to task B, but since we dont have to sort so one map reduce job is enough
 * to compute the connectedness factor for each page owner
 *
 * two mappers emit personId as the key from pages csv and friends csv
 * the reducer joins the data and counts friend occurrences per person
 *
 * this is already optimal ans scalable, the single job approach minimize overhead while efficiently parallelizing the counting operation
 * accross reducers
 */

public class TaskD {
    public static class PagesMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text k = new Text();
        private final Text v = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("PersonID")) return;

            String[] f = line.split(",", -1);
            if (f.length < 5) return;

            String personId = f[0].trim();
            String name = f[1].trim();

            if (personId.isEmpty()) return;

            k.set(personId);
            v.set("P|" + name);
            context.write(k, v);
        }
    }

    public static class FriendsMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text k = new Text();
        private static final Text v = new Text("F|1");

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("FriendsRel")) return; // skip header

            String[] f = line.split(",", -1);
            if (f.length < 3) return;

            String myFriend = f[2].trim();
            if (myFriend.isEmpty()) return;

            k.set(myFriend);
            context.write(k, v);
        }
    }

    public static class CountReducer extends Reducer<Text, Text, Text, Text> {
        private final Text outV = new Text();

        @Override
        public void reduce(Text personId, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            String name = null;

            for (Text value : values) {
                String s = value.toString();
                if (s.startsWith("F|")) {
                    count += 1;
                } else if (s.startsWith("P|")) {
                    String[] parts = s.split("\\|", -1);
                    if (parts.length >= 2){
                        name = parts[1];
                    }
                }
            }

            if (name == null) name = "";

            outV.set(name + "\t" + count);
            context.write(personId, outV);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3){
            System.err.println("Usage: TaskD <pagesPath> <friendsPath> <outputPath>");
            System.exit(2);
        }

        String pagesPath = args[0];
        String friendsPath = args[1];
        String outputPath = args[2];

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TaskD-Connectedness");
        job.setJarByClass(TaskD.class);

        // Multiple inputs
        MultipleInputs.addInputPath(job, new Path(pagesPath), TextInputFormat.class, PagesMapper.class);
        MultipleInputs.addInputPath(job, new Path(friendsPath), TextInputFormat.class, FriendsMapper.class);

        job.setReducerClass(CountReducer.class);

        // Map output types
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Final output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
