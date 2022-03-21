import hadoop.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

/*
    got csv file from : https://www.ncei.noaa.gov/data/global-hourly/archive/csv/ =>2022.tar.gz
    Extract it and put it into hadoop dfs, & packing the app1 run
    ~/target/$ hadoop jar minMaxTemperatures-1.0-Any.jar Application /path_to_your_csv_file_temperatures.csv /output4

    for me:
    ~/target/$ hadoop jar minMaxTemperatures-1.0-Any.jar Application /99999927516.csv /output4
 */
public class Application {

    public static void main(String[] args) throws IOException {
        JobConf conf=new JobConf();
        conf.setJobName("Max & Min Temperatures");
        conf.setJarByClass(Application.class);

        conf.setMapperClass(MyMapper.class);
        conf.setReducerClass(MyReducer.class);

        conf.setMapOutputKeyClass( Text.class);
        conf.setMapOutputValueClass( DoubleWritable.class); //#

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.addInputPath(conf,new Path(args[0]));
        FileOutputFormat.setOutputPath(conf,new Path(args[1]));
        JobClient.runJob(conf);
    }
}