
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import java.io.IOException;

/*
    * Commands :
        (put prenoms.txt) $ hdfs dfs -put prenoms.txt /
    * compile & get .jar file & then run ;
        hadoop jar TP_MapReduce-1.0-ANY.jar Application  /prenoms.txt /output
    * seek result in /output directory
 */

public class Application {
    public static void main(String[] args) throws IOException {
        JobConf conf=new JobConf();
        conf.setJobName("Nomre de mots");
        conf.setJarByClass(Application.class);

        conf.setMapperClass(OccurencesMapper.class);
        conf.setReducerClass(OccurencesReducer.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.addInputPath(conf,new Path(args[0]));
        FileOutputFormat.setOutputPath(conf,new Path(args[1]));

        JobClient.runJob(conf);
    }
}