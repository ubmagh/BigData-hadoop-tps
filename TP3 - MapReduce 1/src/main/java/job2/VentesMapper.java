package job2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Arrays;

public class VentesMapper extends MapReduceBase implements Mapper< LongWritable, Text, MyCustomKey, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, OutputCollector<MyCustomKey, IntWritable> output, Reporter reporter) throws IOException {
        String ventes[] = value.toString().split(" ");
        System.out.println("Splitter => "+ Arrays.toString(ventes));
        MyCustomKey venteKey = new MyCustomKey(  );
        venteKey.setCity( ventes[1] );
        venteKey.setYear( Integer.parseInt( ventes[0]) );
        System.out.println("=> here in mapper ");
        output.collect( venteKey, new IntWritable(Integer.parseInt( ventes[3] )));
    }


}
