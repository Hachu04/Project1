import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * use two map reduce jobs to identify people who are more popular than average
 *
 * job 1: count friends per person (same as task D)
 *
 * job 2: calculate average and filter
 * mapper reads job 1 output and emits all records with all records with key all
 * reducer calculates total friends and total people to compute average
 * reducer filers and outputs only people with count greater than average
 *
 * this two-job approach is necessary because we cannot calculate the global average
 * until all individual counts are compute, requiring two passes over the data
 */

public class TaskH {
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
            if (line.startsWith("FriendRel")) return;

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
                    if (parts.length >= 2) {
                        name = parts[1];
                    }
                }
            }

            if (name == null) name = "";

            outV.set(name + "\t" + count);
            context.write(personId, outV);
        }
    }

    public static class FilterMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] f = line.split("\t", -1);
            if (f.length < 3) return;

            String personId = f[0].trim();
            String name = f[1].trim();
            String countStr = f[2].trim();

            context.write(new Text("all"), value);
        }
    }

    public static class AverageReducer extends Reducer<Text, Text, Text, Text> {
        private final Text outK = new Text();
        private final Text outV = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String[]> records = new ArrayList<>();
            int totalFriends = 0;
            int totalPeople = 0;

            for (Text value : values) {
                String line = value.toString();
                String[] f = line.split("\t", -1);
                if (f.length < 3) continue;

                String personId = f[0].trim();
                String name = f[1].trim();
                int count;
                try {
                    count = Integer.parseInt(f[2].trim());
                } catch (Exception e) {
                    continue;
                }

                records.add(new String[]{personId, name, String.valueOf(count)});
                totalFriends += count;
                totalPeople += 1;
            }

            if (totalPeople == 0) return;

            double average = (double) totalFriends / totalPeople;

            for (String[] record : records) {
                int count = Integer.parseInt(record[2]);
                if (count > average) {
                    outK.set(record[0]);
                    outV.set(record[1] + "\t" + count);
                    context.write(outK, outV);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: TaskH <pagesPath> <friendsPath> <tmpOutput> <finalOutput>");
            System.exit(2);
        }

        String pagesPath = args[0];
        String friendsPath = args[1];
        String tmpOutput = args[2];
        String finalOutput = args[3];

        Configuration conf = new Configuration();

        Job job1 = Job.getInstance(conf, "TaskH-CountFriends");
        job1.setJarByClass(TaskH.class);

        MultipleInputs.addInputPath(job1, new Path(pagesPath), TextInputFormat.class, PagesMapper.class);
        MultipleInputs.addInputPath(job1, new Path(friendsPath), TextInputFormat.class, FriendsMapper.class);

        job1.setReducerClass(CountReducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job1, new Path(tmpOutput));

        if (!job1.waitForCompletion(true)) System.exit(1);

        Job job2 = Job.getInstance(conf, "TaskH-FilterByAverage");
        job2.setJarByClass(TaskH.class);

        job2.setMapperClass(FilterMapper.class);
        job2.setReducerClass(AverageReducer.class);
        job2.setNumReduceTasks(1);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path(tmpOutput));
        FileOutputFormat.setOutputPath(job2, new Path(finalOutput));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}