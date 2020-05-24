package bigdata.job3;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Job3FilesJoin extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: Job-3 <in smallFile> <in bigFile> <out dir>");
            System.exit(2);
        }
        
        String smallInput = args[0];
        Path bigInput = new Path(args[1]);
        Path output = new Path(args[2]);
        Configuration conf = getConf();

        Job job = Job.getInstance(conf, "FilesJoin");
        job.setJarByClass(Job3FilesJoin.class);

        job.addCacheFile(new URI(smallInput));
        FileInputFormat.addInputPath(job, bigInput);
        FileOutputFormat.setOutputPath(job, output);

        job.setMapperClass(MapSideJoinMapper.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new Job3FilesJoin(), args);
        System.exit(exitCode);
    }
    
}