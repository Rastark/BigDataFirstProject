package mapreduce.job2;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import mapreduce.job2.mapper.MapSideJoinMapper;
import mapreduce.job2.mapper.SectorYearMapper;
import mapreduce.job2.objects.HsHspJoinWritable;
import mapreduce.job2.objects.StringBigram;
import mapreduce.job2.reducer.SectorYearReducer;

public class SectorYearJob extends Configured implements Tool{

    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 3) {
            System.err.println("Usage: Job-3 <in smallFile> <in bigFile> <out dir>");
            System.exit(2);
        }

        String smallFile = args[0];
        Path bigFile = new Path(args[1]);
        Path tmpDir = new Path("output/tmp");
        Path outputDir = new Path(args[2]);

        Configuration conf = getConf();

        // MapSideJoinJob
        Job joinJobbe = Job.getInstance(conf, "Job2Join");
        joinJobbe.setJarByClass(SectorYearJob.class);

        joinJobbe.setMapperClass(MapSideJoinMapper.class);
        joinJobbe.addCacheFile(new URI(smallFile));
        joinJobbe.setNumReduceTasks(0);

        FileInputFormat.addInputPath(joinJobbe, bigFile);
        FileOutputFormat.setOutputPath(joinJobbe, tmpDir);

        joinJobbe.setOutputKeyClass(Text.class);
        joinJobbe.setOutputValueClass(Text.class);

        joinJobbe.waitForCompletion(true);

        // SectorYearJob
        Job job = Job.getInstance(conf, "Job2");
        job.setJarByClass(SectorYearJob.class);

        job.setMapperClass(SectorYearMapper.class);
        job.setReducerClass(SectorYearReducer.class);

        FileInputFormat.addInputPath(job, tmpDir);
        FileOutputFormat.setOutputPath(job, outputDir);

        job.setOutputKeyClass(Text.class);
        job.setMapOutputKeyClass(StringBigram.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputValueClass(HsHspJoinWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }      
}

