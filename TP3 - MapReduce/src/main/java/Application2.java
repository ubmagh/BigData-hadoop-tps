import job2.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import java.io.IOException;

/*
    $ cd target
    $ hadoop jar tp3mapreduce-1.0-ANY.jar Application2 /ventes.txt /output2
    => get results in /output2/part****
 */
public class Application2 {
    public static void main(String[] args) throws IOException {
        JobConf conf=new JobConf();
        conf.setJobName("Sales Sum groupedBy year & City");
        conf.setJarByClass(Application2.class);

        conf.setMapperClass(VentesMapper.class);
        conf.setReducerClass(VentesReducer.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapOutputKeyClass( MyCustomKey.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.addInputPath(conf,new Path(args[0]));
        FileOutputFormat.setOutputPath(conf,new Path(args[1]));
        JobClient.runJob(conf);
    }
}
