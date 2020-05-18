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
  public void map(final LongWritable key, final Text value, final Context context)
      throws IOException, InterruptedException {

    context.getCounter(MYCOUNTER.RECORD_COUNT).increment(1);

    if (value.toString().length() > 0) {
      final String arrCompAttributes[] = value.toString().split(",");

      try {
        strFileTicker = FileMap.get(arrCompAttributes[0].toString())[0];
        strFileName = FileMap.get(arrCompAttributes[0].toString())[2];
      } finally {
        strFileTicker = ((strFileTicker.equals(null) || strFileTicker.equals("")) ? "NOT_FOUND" : strFileTicker);
        strFileName = ((strFileName.equals(null) || strFileName.equals("")) ? "NOT_FOUND" : strFileName);
      }

      txtMapOutputKey.set(arrCompAttributes[3].toString());

      // (ticker, name, close, volume, date)
      txtMapOutputValue.set(strFileTicker + "," + strFileName + "," + arrCompAttributes[1].toString() + ","
          + arrCompAttributes[4].toString() + "," + arrCompAttributes[5].toString());
    }
    context.write(txtMapOutputKey, txtMapOutputValue);
    strFileTicker = "";
  }
}