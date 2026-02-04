import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * use a single map reduce job with 3 mappers
 * similar to task D but with an additional mapper to track page visits
 *
 * three mappers emit personId as key
 * PageMapper emit person names
 * FriendsMapper emit which pages each person visited
 *
 * the reducer joins all three data source and compares:
 * if a person declared someone as friend but never visited their page, output that name and id
 *
 * this is optimal and scalable, a single job handles the join and filtering efficiently
 * without needing multiple passes over the data
 */

public class TaskF {
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
        private final Text v = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("FriendRel")) return;

            String[] f = line.split(",", -1);
            if (f.length < 3) return;

            String personId = f[1].trim();
            String myFriend = f[2].trim();

            if (personId.isEmpty() || myFriend.isEmpty()) return;

            k.set(personId);
            v.set("F|" + myFriend);
            context.write(k, v);
        }
    }

    public static class AccessLogMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text k = new Text();
        private final Text v = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("AccessID")) return;

            String[] f = line.split(",", -1);
            if (f.length < 5) return;

            String byWho = f[1].trim();
            String whatPage = f[2].trim();

            if (byWho.isEmpty() || whatPage.isEmpty()) return;

            k.set(byWho);
            v.set("A|" + whatPage);
            context.write(k, v);
        }
    }

    public static class FilterReducer extends Reducer<Text, Text, Text, Text> {
        private final Text outV = new Text();

        @Override
        public void reduce(Text personId, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String name = null;
            Set<String> friends = new HashSet<>();
            Set<String> visited = new HashSet<>();

            for (Text value : values) {
                String s = value.toString();
                if (s.startsWith("P|")) {
                    String[] parts = s.split("\\|", -1);
                    if (parts.length >= 2) {
                        name = parts[1];
                    }
                } else if (s.startsWith("F|")) {
                    String[] parts = s.split("\\|", -1);
                    if (parts.length >= 2) {
                        friends.add(parts[1]);
                    }
                } else if (s.startsWith("A|")) {
                    String[] parts = s.split("\\|", -1);
                    if (parts.length >= 2) {
                        visited.add(parts[1]);
                    }
                }
            }

            if (name == null) return;

            for (String friend : friends) {
                if (!visited.contains(friend)) {
                    outV.set(name);
                    context.write(personId, outV);
                    return;
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: TaskF <pagesPath> <friendsPath> <accessLogPath> <outputPath>");
            System.exit(2);
        }

        String pagesPath = args[0];
        String friendsPath = args[1];
        String accessLogPath = args[2];
        String outputPath = args[3];

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TaskF-UnvisitedFriends");
        job.setJarByClass(TaskF.class);

        MultipleInputs.addInputPath(job, new Path(pagesPath), TextInputFormat.class, PagesMapper.class);
        MultipleInputs.addInputPath(job, new Path(friendsPath), TextInputFormat.class, FriendsMapper.class);
        MultipleInputs.addInputPath(job, new Path(accessLogPath), TextInputFormat.class, AccessLogMapper.class);

        job.setReducerClass(FilterReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}