import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

public class Q12 {
    /*
        delete the directory /BDCC/JAVA
     */
    public static void main(String[] args) {
        Configuration conf = ConnectionToHDFS.getConf();

        try{
            FileSystem fs = FileSystem.get( URI.create(ConnectionToHDFS.getUrl()), conf);
            Path file = new Path("/BDCC/JAVA");

            if ( fs.delete( file, true) )
                System.out.println("[i]  directory deleted ✅");
            else
                System.out.println("[i]  can't delete directory  ❌");

        }catch(Exception exc){
            System.err.println("!!O0ops Exception!! \n => "+exc.getMessage());
            exc.printStackTrace();
        }
    }
}
