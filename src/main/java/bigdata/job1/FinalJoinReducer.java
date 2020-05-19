package bigdata.job1;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FinalJoinReducer extends Reducer<Text, Text, Text, Text> {

    private Map<Text, StatStock> statsMap = new HashMap<>();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        
        StatStock stats =  new StatStock();
        
        for (Text val : values) {
            String[] parts = val.toString().split(":");
            switch (parts[0]) {
                case "change":
                    stats.setChange(new Text(parts[1]));
                    break;
                case "min":
                    stats.setMin(new Text(parts[1]));
                    break;
                case "max":
                    stats.setMax(new Text(parts[1]));
                    break;
                case "volume":
                    stats.setVolume(new Text(parts[1]));
                    break;
                default:
                    break;
            }
        }

        statsMap.put(new Text(key), stats);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Map<Text, StatStock> sortedMap = invertedSortByChange(statsMap);
        for (Text key : sortedMap.keySet()) {
            Text value = new Text(sortedMap.get(key).toString());
            context.write(key, value);
        }
    }


    private static <K extends Text, V extends StatStock> Map<K,V> invertedSortByChange(Map<K,V> map) {
        List<Entry<K,V>> entries = new LinkedList<>(map.entrySet());

        Collections.sort(entries, new Comparator<Entry<K,V>>() {
            @Override
            public int compare(Entry<K, V> o1, Entry<K, V> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });

        Map<K,V> sortedMap = new LinkedHashMap<>();
        for (Entry<K,V> entry : entries) {
            sortedMap.put(entry.getKey(), entry.getValue());
        }
        return sortedMap;
    }
}