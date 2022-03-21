package app2;

import entities.Employe;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class MyReducer extends MapReduceBase implements Reducer<Text, Employe, Text, Text>  {
    @Override
    public void reduce(Text key, Iterator<Employe> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {

        int count = 0;
        while( iterator.hasNext()) {
            iterator.next();
            count ++;
        }
        outputCollector.collect( key, new Text(""+count) );
    }
}
