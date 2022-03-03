import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedReader;
import java.net.URI;

public class Q6 {
    /*
        rename first 2 files & delete the third in /BDCC/JAVA/Cours/.
     */
    public static void main(String[] args) {
        Configuration conf = ConnectionToHDFS.getConf();

        try{
            FileSystem fs = FileSystem.get( URI.create(ConnectionToHDFS.getUrl()), conf);
            Path Cours1old = new Path("/BDCC/JAVA/Cours/Cours1CPP"),
                 Cours2old = new Path("/BDCC/JAVA/Cours/Cours2CPP"),
                 Cours1new = new Path("/BDCC/JAVA/Cours/Cours1JAVA"),
                 Cours2new = new Path("/BDCC/JAVA/Cours/Cours2JAVA"),
                 Cours3 = new Path("/BDCC/JAVA/Cours/Cours3CPP");

            if ( fs.rename( Cours1old, Cours1new) ) //rename first file
                System.out.println("[i] first file renamed ✅");
            else
                System.out.println("[i]  can't rename first file  ❌");

            if ( fs.rename( Cours2old, Cours2new) ) //rename second file
                System.out.println("[i] second file renamed ✅");
            else
                System.out.println("[i]  can't rename second file  ❌");

            fs.delete( Cours3, true); // delete third file
            System.out.println("[i] third file renamed deleted ✅");

        }catch(Exception exc){
            System.err.println("!!O0ops Exception!! \n => "+exc.getMessage());
            exc.printStackTrace();
        }


    }

}
