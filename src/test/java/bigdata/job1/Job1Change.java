package bigdata.job1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Job1Change extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: Job-1-change <in file> <out dir>");
            System.exit(2);
        }
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        Configuration conf =  getConf();
        
        Job job = Job.getInstance(conf, "Job-1-change");
        job.setJarByClass(Job1Change.class);

        job.setMapperClass(ChangePercentageMapper.class);
        job.setReducerClass(ChangePercentageReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DatePrice.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new Job1Change(), args);
        System.exit(exitCode);
    }
    
}