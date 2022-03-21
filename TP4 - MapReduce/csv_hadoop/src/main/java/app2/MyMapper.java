package app2;

import entities.Employe;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class MyMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Employe> {

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Employe> output, Reporter reporter) throws IOException {
        String data[] = value.toString().split(",");
        Employe employe = new Employe( data[1], data[0], data[2], data[3], Double.parseDouble(data[4]) );
        output.collect( new Text(employe.getDepartement()), employe );
    }

}
