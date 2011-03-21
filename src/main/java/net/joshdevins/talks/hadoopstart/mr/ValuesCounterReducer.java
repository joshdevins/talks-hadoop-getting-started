package net.joshdevins.talks.hadoopstart.mr;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public final class ValuesCounterReducer extends Reducer<Text, NullWritable, NullWritable, Text> {

    @Override
    public void reduce(final Text key, final Iterable<NullWritable> values, final Context context) throws IOException,
            InterruptedException {

        Iterator<NullWritable> iterator = values.iterator();
        int count = 0;
        while (iterator.hasNext()) {
            count++;
            iterator.next();
        }

        StringBuilder sb = new StringBuilder();
        sb.append(key.toString());
        sb.append(",");
        sb.append(count);

        context.write(NullWritable.get(), new Text(sb.toString()));
    }
}
