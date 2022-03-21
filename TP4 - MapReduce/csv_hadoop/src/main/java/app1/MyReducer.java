package app1;

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
        Employe maxSalaryEmploye=new Employe(), minSalaryEmploye=new Employe(), tmpEmploye;
        maxSalaryEmploye.setSalary(0);
        minSalaryEmploye.setSalary(Double.MAX_VALUE);
        while( iterator.hasNext()) {
            tmpEmploye = iterator.next();
            if( tmpEmploye.getSalary() > maxSalaryEmploye.getSalary() )
                maxSalaryEmploye = tmpEmploye;
            if( tmpEmploye.getSalary()< minSalaryEmploye.getSalary() )
                minSalaryEmploye = tmpEmploye;
        }
        outputCollector.collect( key, new Text("[ max: "+maxSalaryEmploye+", min: "+minSalaryEmploye+" ]") );
    }
}
