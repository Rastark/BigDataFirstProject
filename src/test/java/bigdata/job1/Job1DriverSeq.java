package bigdata.job1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Job1DriverSeq extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: Job-1 <in file> <out dir>");
            System.exit(2);
        }

        Path input = new Path(args[0]);
        Path temp1 = new Path("output/temp1-change");
        Path temp2 = new Path("output/temp2-min");
        Path temp3 = new Path("output/temp3-max");
        Path temp4 = new Path("output/temp4-volume");
        Path output = new Path(args[1]);
        Configuration conf =  getConf();
        
    /* ========== INIT JOB CHANGE PERCENTAGE ========== */

        Job job1 = Job.getInstance(conf, "Job-1-change");
        job1.setJarByClass(Job1DriverSeq.class);

        FileInputFormat.addInputPath(job1, input);
        FileOutputFormat.setOutputPath(job1, temp1);

        job1.setMapperClass(ChangePercentageMapper.class);
        job1.setReducerClass(ChangePercentageReducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(DatePrice.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        System.out.println("*****************************************************"
                         + "**********           START JOB 1           **********"
                         + "*****************************************************");
        // job1.submit();
        if (job1.waitForCompletion(true))
            System.out.println("*****************************************************"
                             + "**********            END JOB 1            **********"
                             + "*****************************************************");
        else
            return 1;


    /* ==========     INIT JOB MIN PRICE     ========== */
        
        Job job2 = Job.getInstance(conf, "Job-1-min");
        job2.setJarByClass(Job1DriverSeq.class);

        FileInputFormat.addInputPath(job2, input);
        FileOutputFormat.setOutputPath(job2, temp2);

        job2.setMapperClass(MinPriceMapper.class);
        job2.setCombinerClass(MinPriceCombiner.class);
        job2.setReducerClass(MinPriceReducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(DoubleWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        System.out.println("*****************************************************"
                         + "**********           START JOB 2           **********"
                         + "*****************************************************");
        // job2.submit();
        if (job2.waitForCompletion(true))
            System.out.println("*****************************************************"
                             + "**********            END JOB 2            **********"
                             + "*****************************************************");
        else
            return 1;


    /* ==========     INIT JOB MAX PRICE     ========== */

        Job job3 = Job.getInstance(conf, "Job-1-max");
        job3.setJarByClass(Job1DriverSeq.class);

        FileInputFormat.addInputPath(job3, input);
        FileOutputFormat.setOutputPath(job3, temp3);

        job3.setMapperClass(MaxPriceMapper.class);
        job3.setCombinerClass(MaxPriceCombiner.class);
        job3.setReducerClass(MaxPriceReducer.class);

        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(DoubleWritable.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        System.out.println("*****************************************************"
                         + "**********           START JOB 3           **********"
                         + "*****************************************************");
        // job3.submit();
        if (job3.waitForCompletion(true))
            System.out.println("*****************************************************"
                             + "**********            END JOB 3            **********"
                             + "*****************************************************");
        else
            return 1;


    /* ==========    INIT JOB MEAN VOLUME    ========== */

        Job job4 = Job.getInstance(conf, "Job-1-volume");
        job4.setJarByClass(Job1DriverSeq.class);

        FileInputFormat.addInputPath(job4, input);
        FileOutputFormat.setOutputPath(job4, temp4);

        job4.setMapperClass(MeanVolumeMapper.class);
        job4.setReducerClass(MeanVolumeReducer.class);

        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(LongWritable.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);

        System.out.println("*****************************************************"
                         + "**********           START JOB 4           **********"
                         + "*****************************************************");
        // job4.submit();
        if (job4.waitForCompletion(true))
            System.out.println("*****************************************************"
                             + "**********            END JOB 4            **********"
                             + "*****************************************************");
        else
            return 1;


    /* ==========       INIT JOB JOIN       =========== */

        // boolean succ = false;
        // do {
        //     succ = job1.isSuccessful() && job2.isSuccessful()
        //         && job3.isSuccessful() && job4.isSuccessful();
        // } while (!succ);

        // while (job1.isSuccessful() && job2.isSuccessful()
        //     && job3.isSuccessful() && job4.isSuccessful()) {
        //         if ()
        // }

        Job job5 = Job.getInstance(conf, "Job-1-join");
        job5.setJarByClass(Job1DriverSeq.class);

        FileInputFormat.setInputPaths(job5, temp1, temp2, temp3, temp4);
        FileOutputFormat.setOutputPath(job5, output);

        job5.setMapperClass(FinalJoinMapper.class);
        job5.setReducerClass(FinalJoinReducer.class);

        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(Text.class);

        System.out.println("*****************************************************"
                         + "**********           START JOB 5           **********"
                         + "*****************************************************");
        // return job5.waitForCompletion(true) ? 0 : 1;
        if (job5.waitForCompletion(true)) {
            System.out.println("*****************************************************"
                             + "**********            END JOB 5            **********"
                             + "*****************************************************");
            return 0;
        } else{
            return 1;
        }

    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new Job1DriverSeq(), args);
        System.exit(exitCode);
    }
    
}