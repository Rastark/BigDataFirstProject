package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import mapreduce.job.SectorYearJob;

/**
 * Launches MapJoinJob
 *
 */
public class Job2Driver {
    public static void main(final String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new SectorYearJob(), args);
        System.exit(exitCode);
    }
}
