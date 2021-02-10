package io.dev.filtering.conditional;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Map extends Mapper<LongWritable, Text, NullWritable,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //Getting the required data to apply conditional
        String maritalStatus = value.toString().split(",")[5].trim();
        //Conditional Statement is added here so that only the row which has valid data will be inserted/added into
        //the context so that it can be processed latter.
        if (maritalStatus.equalsIgnoreCase("Married-civ-spouse"))
            context.write(NullWritable.get(), value);
    }
}