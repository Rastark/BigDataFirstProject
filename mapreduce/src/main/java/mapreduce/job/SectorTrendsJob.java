package mapreduce.job;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import mapreduce.mapper.MapSideJoinMapper;
import mapreduce.reducer.MapSideJoinReducer;

public class SectorTrendsJob {

    public static void launchSectorTrendsJob(final String in, final String joinFilePath, final String out)
            throws Exception {

        final Job job = Job.getInstance();
        job.setJarByClass(SectorJob.class);

        job.setMapperClass(MapSideJoinMapper.class);
        job.addCacheFile(new Path(joinFilePath).toUri());

        FileInputFormat.addInputPath(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);
    }
}