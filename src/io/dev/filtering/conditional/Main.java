package io.dev.filtering.conditional;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Main extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(getConf());

        job.setJobName("ConditionalFiltering");
        job.setJarByClass(Main.class);

        //Setting the Map phase outputs
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        //Setting the Map phase outputs
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        //We can use the same reducer logic here
        //No change if we merge before hand
        job.setCombinerClass(Reduce.class);

        Path inputFolderPath = new Path(strings[0]);
        Path outputFolderPath = new Path(strings[1]);

        FileInputFormat.addInputPath(job,inputFolderPath);
        FileOutputFormat.setOutputPath(job,outputFolderPath);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int code = ToolRunner.run(new Main(),args);
        System.exit(code);

    }
}
