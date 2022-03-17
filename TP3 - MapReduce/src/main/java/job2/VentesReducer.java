package job2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class VentesReducer extends MapReduceBase implements Reducer<MyCustomKey, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(MyCustomKey key, Iterator<IntWritable> iterator, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
        int somme =0;
        while( iterator.hasNext()) {
            somme += iterator.next().get();
        }
        String labelStr = key.getCity()+"@"+key.getYear();
        Text label = new Text( labelStr );
        outputCollector.collect( label, new IntWritable(somme) );
    }
}