package io.dev;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Main extends Configured implements Tool{

    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(getConf());

        job.setJobName("SummaryMetricsData");
        job.setJarByClass(Main.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        Path inputFolderPath = new Path(strings[0]);
        Path outputFolderPath = new Path(strings[1]);

        FileInputFormat.addInputPath(job,inputFolderPath);
        FileOutputFormat.setOutputPath(job,outputFolderPath);


        return job.waitForCompletion(true) ? 0 : 1 ;
    }

    public static void main(String[] args) throws Exception {
        int exit_code = ToolRunner.run(new Main(),args);
        System.exit(exit_code);

    }
}