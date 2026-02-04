import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * use two MapReduce jobs
 * Job 1:
 * two mappers read pages and access_logs, both emit pageId as key
 * reducer groups by pageId, joins page metadata with access records, and count total accesses per page
 * this is scalable because counting is done in parallel across partitions and only grouped once
 *
 * Job 2:
 * Mapper initialize key by negative to sort descending
 * single reducer keeps only 10 records
 * this minimizes output and avoids storing full sorted datasets
 *
 * Why:
 * a single job can not both aggregate and rank efficiently, so a two-stage pipeline is the optimal
 * scalable approach
 */

public class TaskB {
    public static class PagesMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text k = new Text();
        private final Text v = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("PersonID")) return;

            String[] f = line.split(",", -1);
            if (f.length < 5) return;

            String pageId = f[0].trim();
            String name = f[1].trim();
            String nationality = f[2].trim();

            if (pageId.isEmpty()) return;

            k.set(pageId);
            v.set("P|" + name + "|" + nationality);
            context.write(k, v);
        }
    }

    public static class AccessLogMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text k = new Text();
        private static final Text v = new Text("A|1");

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("AccessID")) return;

            String[] f = line.split(",", -1);
            if (f.length < 5) return;

            String whatPage = f[2].trim();
            if (whatPage.isEmpty()) return;

            k.set(whatPage);
            context.write(k, v);
        }
    }

    public static class JoinCountReducer extends Reducer<Text, Text, Text, Text> {
        private final Text outV = new Text();

        public void reduce(Text pageId, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            String name = null;
            String nationality = null;

            for (Text tv : values) {
                String s = tv.toString();
                if (s.startsWith("A|")) {
                    count += 1;
                } else if (s.startsWith("P|")) {
                    String[] parts = s.split("\\|", -1);
                    if (parts.length >= 3) {
                        name = parts[1];
                        nationality = parts[2];
                    }
                }
            }

            if (name == null) name = "";
            if (nationality == null) nationality = "";

            outV.set(count + "\t" + name + "\t" + nationality);
            context.write(pageId, outV);
        }
    }

    // Job 2: Global top 10 by count 1 reducer
    public static class Top10Mapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private final IntWritable outK = new IntWritable();
        private final Text outV = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // input line from job1: pageId count name nationality
            String[] p = value.toString().split("\t", -1);
            if (p.length < 4) return;

            String pageId = p[0].trim();
            int count;
            try {
                count = Integer.parseInt(p[1].trim());
            } catch (Exception e) {
                return;
            }

            String name = p[2];
            String nationality = p[3];

            outK.set(-count);
            outV.set(pageId + "\t" + name + "\t" + nationality + "\t" + count);
            context.write(outK, outV);
        }
    }

    public static class Top10Reducer extends Reducer<IntWritable, Text, Text, NullWritable> {
        private int emitted = 0;
        private final Text out = new Text();

        @Override
        public void reduce(IntWritable negCount, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            if (emitted >= 10) return;

            Iterator<Text> it = values.iterator();

            while (it.hasNext() && emitted < 10) {
                out.set(it.next().toString());
                context.write(out, NullWritable.get());
                emitted++;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4){
            System.err.println("Usage: TaskB <pagesPath> <accessLogPath> <tmpOut> <finalOut>");
            System.exit(2);
        }

        String pagesPath = args[0];
        String accessPath = args[1];
        String tmpOut = args[2];
        String finalOut = args[3];

        Configuration conf = new Configuration();

        Job job1 = Job.getInstance(conf, "TaskB-join-count");
        job1.setJarByClass(TaskB.class);

        // read file line by line
        MultipleInputs.addInputPath(job1, new Path(pagesPath), TextInputFormat.class, PagesMapper.class);
        MultipleInputs.addInputPath(job1, new Path(accessPath), TextInputFormat.class, AccessLogMapper.class);

        // after all mappers finish, hadoop group by key and calls join count reducer
        job1.setReducerClass(JoinCountReducer.class);

        // define mapper output types
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class); // text pageid, text tagged value

        // define final output types
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class); // page id, count + name + nationality

        FileOutputFormat.setOutputPath(job1, new Path(tmpOut));

        // run job1
        if (!job1.waitForCompletion(true)) System.exit(1);

        Job job2 = Job.getInstance(conf, "TaskB-top10");
        job2.setJarByClass(TaskB.class);

        job2.setMapperClass(Top10Mapper.class);
        job2.setReducerClass(Top10Reducer.class);
        job2.setNumReduceTasks(1); // global top 10

        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job2, new Path(tmpOut));
        FileOutputFormat.setOutputPath(job2, new Path(finalOut));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
