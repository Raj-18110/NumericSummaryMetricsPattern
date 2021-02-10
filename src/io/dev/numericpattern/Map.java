package io.dev.numericpattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Map extends Mapper<LongWritable, Text,Text, SumCountWritable>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //5th Index contains marital status
        String marital_status = value.toString().split(",")[5].trim();
        //12th index contains weekly working hours of individuals
        double hours = Double.parseDouble(value.toString().split(",")[12].trim());
        context.write(new Text(marital_status),new SumCountWritable(hours,1d));
    }
}