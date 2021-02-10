package io.dev.numericpattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Combiner extends Reducer<Text, SumCountWritable,Text,SumCountWritable> {
    @Override
    protected void reduce(Text key, Iterable<SumCountWritable> values, Context context) throws IOException, InterruptedException {
        double sum = 0 ,count = 0;
        //Finding average working hours on the aggregration based on marital status
        //Total working hours of the common individual marital status divided by number of people
        //Coming under that marital status
        for(SumCountWritable value : values){
            sum += value.getSum().get();
            count+=value.getCount().get();
        }
        context.write(key,new SumCountWritable(sum,count));
    }
}
