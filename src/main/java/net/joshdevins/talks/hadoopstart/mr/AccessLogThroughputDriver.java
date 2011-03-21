package net.joshdevins.talks.hadoopstart.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AccessLogThroughputDriver extends Configured implements Tool {

    @Override
    public int run(final String[] args) throws Exception {

        if (args.length != 2) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n", AccessLogThroughputDriver.class
                    .getSimpleName()); // NOPMD
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Job job = createJob(args);
        job.setJarByClass(AccessLogThroughputDriver.class);

        // set the input and output paths
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // submit job and wait for completion
        boolean success = job.waitForCompletion(true);

        return success ? 0 : -1;
    }

    private Job createJob(final String[] args) throws IOException {

        Job job = new Job(getConf(), "Apache Access Log : Throughput");

        // DFS inputs and outputs
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // mapper and reducer
        job.setMapperClass(AccessLogThroughputParseGroupBySecondsMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setReducerClass(ValuesCounterReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(2);

        return job;
    }

    public static void main(final String[] args) throws Exception { // NOPMD by devins

        int status = ToolRunner.run(new Configuration(), new AccessLogThroughputDriver(), args);

        if (status != 0) {
            System.exit(status);
        }
    }
}
