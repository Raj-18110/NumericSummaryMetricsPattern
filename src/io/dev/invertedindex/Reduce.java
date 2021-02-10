package io.dev.invertedindex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class Reduce extends Reducer<Text, Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder stringBuilder = new StringBuilder();
        Iterator val = values.iterator();
        while (val.hasNext()) {
            stringBuilder.append(val.next());
                    if(val.hasNext())
                        stringBuilder.append("|");
        }
        context.write(key,new Text(stringBuilder.toString()));

    }
}
