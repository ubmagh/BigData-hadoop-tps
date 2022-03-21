package hadoop;

import com.opencsv.CSVParser;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import java.io.IOException;

public class MyMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

    boolean skipedOne = false;
    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
        CSVParser parser = new CSVParser();
        String data[] = parser.parseLine(value.toString());

        if( !skipedOne ) {  // jump first line that contains headers
            skipedOne = true;
            return;
        }
        String [] date = data[1].split("-");
        String doubm= data[13].replace('"', ' ').replace(',', '.').trim();
        double temperature = Double.valueOf( doubm );
        output.collect( new Text( date[1] ), new DoubleWritable( temperature ) );
    }

}
