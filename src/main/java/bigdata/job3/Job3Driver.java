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

public class Job3Driver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: Job-3 <in smallFile> <in bigFile> <out dir>");
            System.exit(2);
        }
        
        String smallInput = args[0];
        Path bigInput = new Path(args[1]);
        Path tempJ = new Path("output/temp-join");
        Path temp1 = new Path("output/temp1");
        Path temp2 = new Path("output/temp2");
        Path temp3 = new Path("output/temp3");
        Path output = new Path(args[2]);
        Configuration conf = getConf();

    /* ==========        INIT JOB JOIN        ========== */

        Job jobJoin = Job.getInstance(conf, "Job-3-join");
        jobJoin.setJarByClass(Job3Driver.class);

        jobJoin.addCacheFile(new URI(smallInput));
        FileInputFormat.addInputPath(jobJoin, bigInput);
        FileOutputFormat.setOutputPath(jobJoin, tempJ);

        jobJoin.setMapperClass(MapSideJoinMapper.class);
        jobJoin.setNumReduceTasks(0);

        jobJoin.setOutputKeyClass(Text.class);
        jobJoin.setOutputValueClass(Text.class);

        System.out.println("*****************************************************"
                         + "**********          START JOB JOIN         **********"
                         + "*****************************************************");
        jobJoin.waitForCompletion(true);


    /* ==========         INIT JOB 1         ========== */

        Job job1 = Job.getInstance(conf, "Job-3-1");
        job1.setJarByClass(Job3Driver.class);

        FileInputFormat.addInputPath(job1, tempJ);
        FileOutputFormat.setOutputPath(job1, temp1);

        job1.setMapperClass(Mapper1.class);
        job1.setReducerClass(Reducer1.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        System.out.println("*****************************************************"
                         + "**********           START JOB 1           **********"
                         + "*****************************************************");
        job1.waitForCompletion(true);


    /* ==========         INIT JOB 2         ========== */

        Job job2 = Job.getInstance(conf, "Job-3-2");
        job2.setJarByClass(Job3Driver.class);

        FileInputFormat.addInputPath(job2, temp1);
        FileOutputFormat.setOutputPath(job2, temp2);

        job2.setMapperClass(Mapper2.class);
        job2.setReducerClass(Reducer2.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        System.out.println("*****************************************************"
                         + "**********           START JOB 2           **********"
                         + "*****************************************************");
        job2.waitForCompletion(true);


    /* ==========         INIT JOB 3         ========== */

        Job job3 = Job.getInstance(conf, "Job-3-3");
        job3.setJarByClass(Job3Driver.class);

        FileInputFormat.addInputPath(job3, temp3);
        FileOutputFormat.setOutputPath(job3, output);

        job3.setMapperClass(Mapper3.class);
        job3.setReducerClass(Reducer3.class);

        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        System.out.println("*****************************************************"
                         + "**********           START JOB 3           **********"
                         + "*****************************************************");
        if (job3.waitForCompletion(true)) {
            System.out.println("*****************************************************"
                             + "**********            END JOB 3            **********"
                             + "*****************************************************");
            return 0;
        } else{
            return 1;
        }
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new Job3Driver(), args);
        System.exit(exitCode);
    }
    
}