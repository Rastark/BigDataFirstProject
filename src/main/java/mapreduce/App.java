package mapreduce;

import mapreduce.job.MapJoinJob;

/**
 * Launches MapJoinJob
 *
 */
public class App {
    public static void main(final String[] args) {
        try {
            MapJoinJob.launchMapJoinJob(args[0], args[1], args[2]);
        } catch (final Exception e) {
            System.out.println("Invalid arguments!");
        }
        System.out.println("Hello World!");
    }
}
