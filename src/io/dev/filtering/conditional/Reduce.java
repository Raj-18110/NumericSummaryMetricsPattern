package io.dev.filtering.conditional;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reduce extends Reducer<NullWritable, Text,NullWritable,Text> {
    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //As there is no key so all the data are merged into one list as all have nullwritable
        //So iterating through that list and adding the data into the context for output
        for(Text value : values)
            context.write(NullWritable.get(),value);
    }
}
