package mapreduce;

import mapreduce.job.SectorJob;

/**
 * Launches SectorJob
 *
 */
public class App {
    public static void main(String[] args) {
        try {
            SectorJob.launchExampleJob(args[0], args[1], args[2]);
        } catch (Exception e) {
            System.out.println("Invalid arguments!");
        }
        System.out.println("Hello World!");
    }
}
