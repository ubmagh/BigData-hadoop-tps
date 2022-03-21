package hadoop;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class MyReducer extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, Text> {
    @Override
    public void reduce(Text key, Iterator<DoubleWritable> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
        Double maxTemperature = Double.MIN_VALUE, minTemperature = Double.MAX_VALUE, tmpTemp;

        while (iterator.hasNext()) {
            tmpTemp = iterator.next().get();
            if (tmpTemp > maxTemperature)
                maxTemperature = tmpTemp;
            if (tmpTemp < minTemperature)
                minTemperature = tmpTemp;
        }
        outputCollector.collect(key, new Text("[ max: " + maxTemperature + ", min: " + minTemperature + " ]"));
    }
}