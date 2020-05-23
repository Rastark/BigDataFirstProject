package mapreduce.job;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import mapreduce.mapper.MapSideJoinMapper;
import mapreduce.mapper.SectorYearMapper;
import mapreduce.objects.StringBigram;
import mapreduce.reducer.SectorYearReducer;

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
        joinJobbe.setMapOutputKeyClass(StringBigram.class);
        joinJobbe.setOutputValueClass(Text.class);
        joinJobbe.setMapOutputValueClass(Text.class);

        joinJobbe.waitForCompletion(true);

        // SectorYearJob
        Job job = Job.getInstance(conf, "Job2");
        job.setJarByClass(SectorYearJob.class);

        job.setMapperClass(SectorYearMapper.class);
        job.setReducerClass(SectorYearReducer.class);

        FileInputFormat.addInputPath(job, tmpDir);
        FileOutputFormat.setOutputPath(job, outputDir);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }      
}

