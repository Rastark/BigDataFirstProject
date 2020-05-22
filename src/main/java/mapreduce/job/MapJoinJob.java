package mapreduce.job;

import java.net.URI;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import mapreduce.mapper.MapSideJoinMapper;

public class MapJoinJob {

    public static void launchMapJoinJob(String in, String joinFilePath, String out) throws Exception {

        final Job job = Job.getInstance();
        job.setJarByClass(SectorTrendsJob.class);

        job.setMapperClass(MapSideJoinMapper.class);
        job.addCacheFile(new URI(joinFilePath));
        job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);
    }

}