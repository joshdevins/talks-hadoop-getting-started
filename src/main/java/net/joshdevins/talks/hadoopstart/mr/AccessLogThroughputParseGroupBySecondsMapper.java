package net.joshdevins.talks.hadoopstart.mr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public final class AccessLogThroughputParseGroupBySecondsMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    @Override
    public void map(final LongWritable key, final Text value, final Context context) throws IOException {

        // parse the raw log line
        ApacheCombinedAccessLogEntry entry = ApacheCombinedAccessLogParser.parse(value.toString());

        // check for invalid rows
        if (entry == null) {
            return;
        }

        // do some filtering
        // let's only consider 2010 data, GET requests that return 2xx-3xx
        if (!(entry.getTimestamp().matches("\\d{2}/\\w{3}/2010:.*") && "GET".equals(entry.getMethod())
                && entry.getStatusCode() >= 200 && entry.getStatusCode() <= 399)) {
            return;
        }

        try {
            context.write(new Text(entry.getTimestamp()), NullWritable.get());

        } catch (InterruptedException ie) {
            System.err.println("Interrupted while writing output");
            ie.printStackTrace();
        }
    }
}
