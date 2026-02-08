import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.yarn.webapp.view.HtmlPage;

public class TaskG {

//

    public static class PagesMapper extends Mapper<Object, Text, IntWritable, Text> {
        private IntWritable personID = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] columns = value.toString().split(",");
            if (columns.length > 1 && !columns[0].contains("PersonID")) { // Header check
                try {
                    personID.set(Integer.parseInt(columns[0].trim()));
                    context.write(personID, new Text("person|" + columns[1]));
                } catch (NumberFormatException e) { /* Skip malformed rows */ }
            }
        }
    }

    public static class AccessLogMapper extends Mapper<Object, Text, IntWritable, Text> {
        private IntWritable userID = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] columns = value.toString().split(",");
            if (columns.length > 4 && !columns[0].contains("AccessID")) { // Header check
                try {
                    userID.set(Integer.parseInt(columns[1].trim()));
                    context.write(userID, new Text("access|" + columns[4]));
                } catch (NumberFormatException e) { /* Skip malformed rows */ }
            }
        }
    }

    public static class DisconnectedReducer
            extends Reducer<IntWritable,Text,IntWritable,Text> {

        public void reduce(IntWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            long fourteenDaysInMs = 14L * 24 * 60 * 60 * 1000;
//            long now = System.currentTimeMillis();
            long now = 0;
            try {
                now = format.parse("2024-11-10 00:00:00").getTime();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }

            String name = "";
            long latestAccessTime = 0;

            for (Text value : values) {
                String[] parts = value.toString().split("\\|");
                String relation = parts[0];
                String actualValue = parts[1];

                if (relation.equals("person")) {
                    name = actualValue;
                } else {
                    try {
                        long accessTime = format.parse(actualValue).getTime();
                        if (accessTime > latestAccessTime) {
                            latestAccessTime = accessTime;
                        }
                    } catch (ParseException e) {e.getMessage();};

                }
            }

            if (!name.isEmpty()) {
                if ((now - latestAccessTime) >= fourteenDaysInMs) {
                    context.write(key, new Text(name));
                }
            }
        }
    }

    public void debug(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "task g");
        job.setJarByClass(TaskG.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
//        job.setCombinerClass(DisconnectedReducer.class);
        job.setReducerClass(DisconnectedReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, PagesMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, AccessLogMapper.class);
//        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "task g");
        job.setJarByClass(TaskG.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
//        job.setCombinerClass(DisconnectedReducer.class);
        job.setReducerClass(DisconnectedReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PagesMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, AccessLogMapper.class);
//        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}