package bigdata.job3;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapSideJoinMapper extends Mapper<LongWritable, Text, Text, Text> {

    private HashMap<String, String[]> FileMap = new HashMap<String, String[]>();
    private BufferedReader brReader;
    private String strFileTicker = "";
    private String strFileName = "";
    private String strFileSector = "";
    private Text txtMapOutputKey = new Text("");
    private Text txtMapOutputValue = new Text("");
    // private String ciccio = "";

    public enum MYCOUNTER {
        RECORD_COUNT, HS_FILE_EXISTS, HSP_FILE_EXISTS, FILE_NOT_FOUND, SOME_OTHER_ERROR
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        URI[] cacheFiles = context.getCacheFiles();

        for (URI eachURI : cacheFiles) {
            if (eachURI.getPath().trim().equals("input/dataset-1/hss_cleaned.csv")) {
                context.getCounter(MYCOUNTER.HS_FILE_EXISTS).increment(1);
                loadFileHashMap(new Path("hss_cleaned.csv"), context);
                // this.ciccio = (new Path(eachURI.getPath()).toString());
            }
        }
    }

    private void loadFileHashMap(Path filePath, Context context) throws IOException {

        String strLineRead = "";

        try {
            brReader = new BufferedReader(new FileReader(filePath.toString()));

            // Read each line, split and load to HashMap
            // System.out.println(brReader.readLine());
            while ((strLineRead = brReader.readLine()) != null) {
                String cachedArray[] = strLineRead.split(",");
                String cachedValues[] = Arrays.copyOfRange(cachedArray, 1, cachedArray.length);
                // System.out.println(cachedValues.toString());
                FileMap.put(cachedArray[0].trim(), cachedValues);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            context.getCounter(MYCOUNTER.FILE_NOT_FOUND).increment(1);
        } catch (IOException e) {
            context.getCounter(MYCOUNTER.SOME_OTHER_ERROR).increment(1);
        } finally {
            if (brReader != null) {
                brReader.close();
            }
        }
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        context.getCounter(MYCOUNTER.RECORD_COUNT).increment(1);

        if (value.toString().length() > 0) {
            String arrCompAttributes[] = value.toString().split(",");

            try {
                String compFileKey = arrCompAttributes[0].toString().trim();
                // System.out.println(FileMap.keySet().toString());
                if (FileMap.keySet().contains(compFileKey)) {
                    strFileTicker = compFileKey;
                    strFileName = FileMap.get(compFileKey)[1];
                    strFileSector = FileMap.get(compFileKey)[2];
                    // } else {
                    // strFileName = "pippo";
                }
            } finally {
                strFileTicker = ((strFileTicker.equals(null) || strFileTicker.equals("")) ? "NOT_FOUND"
                        : strFileTicker);
                strFileName = ((strFileName.equals(null) || strFileName.equals("")) ? "NOT_FOUND" : strFileName);
                strFileSector = ((strFileSector.equals(null) || strFileSector.equals("")) ? "NOT_FOUND"
                        : strFileSector);
            }

            // System.out.println(
            // "*****************************************************************************************\n***************************************************************************"
            // + context.getCacheFiles().toString()
            // +
            // "**********************************************************************************\n**********************************************************************************");

            // System.out.println(ciccio);

            txtMapOutputKey.set(arrCompAttributes[0].toString().trim());

            // (ticker, name, sector, close, volume, date)
            txtMapOutputValue.set(strFileName + "," + strFileSector + "," + arrCompAttributes[1].toString() + ","
                    + arrCompAttributes[4].toString() + "," + arrCompAttributes[5].toString());
        }

        // System.out.println();
        context.write(txtMapOutputKey, txtMapOutputValue);
    }
}