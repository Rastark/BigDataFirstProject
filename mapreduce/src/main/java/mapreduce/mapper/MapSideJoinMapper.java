package mapreduce.mapper;

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

    private static HashMap<String, String[]> FileMap = new HashMap<String, String[]>();
    private BufferedReader brReader;
    private String strFileTicker = "";
    private String strFileName = "";
    private final Text txtMapOutputKey = new Text("");
    private final Text txtMapOutputValue = new Text("");

    enum MYCOUNTER {
        RECORD_COUNT, HS_FILE_EXISTS, HSP_FILE_EXISTS, FILE_NOT_FOUND, SOME_OTHER_ERROR
    }

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException {

        final URI[] cacheFiles = context.getCacheFiles();

        for (final URI eachURI : cacheFiles) {
            if (eachURI.getPath().trim().equals("hs_preprocessed")) {
                context.getCounter(MYCOUNTER.HS_FILE_EXISTS).increment(1);
                loadFileHashMap(new Path(eachURI), context);
            }
        }
    }

    private void loadFileHashMap(final Path filePath, final Context context) throws IOException {

        String strLineRead = "";

        try {
            brReader = new BufferedReader(new FileReader(filePath.toString()));

            // Read each line, split and load to HashMap
            while ((strLineRead = brReader.readLine()) != null) {
                final String cachedArray[] = strLineRead.split(",");
                final String cachedValues[] = Arrays.copyOfRange(cachedArray, 1, cachedArray.length - 1);
                FileMap.put(cachedArray[0].trim(), cachedValues);
            }
        } catch (final FileNotFoundException e) {
            e.printStackTrace();
            context.getCounter(MYCOUNTER.FILE_NOT_FOUND).increment(1);
        } catch (final IOException e) {
            context.getCounter(MYCOUNTER.SOME_OTHER_ERROR).increment(1);
        } finally {
            if (brReader != null) {
                brReader.close();
            }
        }
    }

    @Override
    public void map(final LongWritable key, final Text value, final Context context)
            throws IOException, InterruptedException {

        context.getCounter(MYCOUNTER.RECORD_COUNT).increment(1);

        if (value.toString().length() > 0) {
            final String arrCompAttributes[] = value.toString().split(",");

            try {
                strFileTicker = FileMap.keySet().get(arrCompAttributes[0].toString());
                strFileName = FileMap.get(arrCompAttributes[0].toString())[1];
            } finally {
                strFileTicker = ((strFileTicker.equals(null) || strFileTicker.equals("")) ? "NOT_FOUND"
                        : strFileTicker);
                strFileName = ((strFileName.equals(null) || strFileName.equals("")) ? "NOT_FOUND" : strFileName);
            }

            txtMapOutputKey.set(arrCompAttributes[0].toString());

            // (ticker, name, sector, close, volume, date)
            txtMapOutputValue.set(strFileTicker + "," + strFileName + "," + arrCompAttributes[1].toString() + "," + arrCompAttributes[3].toString() + ","
                    + arrCompAttributes[4].toString() + "," + arrCompAttributes[5].toString());
        }
        context.write(txtMapOutputKey, txtMapOutputValue);
        strFileTicker = "";
    }
}