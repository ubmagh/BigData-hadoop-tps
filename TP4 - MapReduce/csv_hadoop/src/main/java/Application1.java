import entities.Employe;
import app1.MyMapper;
import app1.MyReducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

/*
    after cppying employes.csv to dfs, & packing the app1 run
    ~/target/$ hadoop jar csv-hadoop-1.0-Any.jar  Application1 /employes.csv /output3
 */

public class Application1 {

    public static void main(String[] args) throws IOException {
        JobConf conf=new JobConf();
        conf.setJobName("Max & Min salaries");
        conf.setJarByClass(Application1.class);

        conf.setMapperClass(MyMapper.class);
        conf.setReducerClass(MyReducer.class);

        conf.setMapOutputKeyClass( Text.class);
        conf.setMapOutputValueClass( Employe.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.addInputPath(conf,new Path(args[0]));
        FileOutputFormat.setOutputPath(conf,new Path(args[1]));
        JobClient.runJob(conf);
    }
}
