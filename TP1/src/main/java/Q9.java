import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

public class Q9 {
    /*
        Copy files TP1JAVA & TP2JAVA from local directory /Q7 to remote directory /BDCC/JAVA/TPs
     */
    public static void main(String[] args) {
        Configuration conf = ConnectionToHDFS.getConf();

        try{
            FileSystem fs = FileSystem.get( URI.create(ConnectionToHDFS.getUrl()), conf);
            Path TP1r = new Path("/BDCC/JAVA/TPs/TP1JAVA"),
                 TP2r = new Path("/BDCC/JAVA/TPs/TP2JAVA"),
                 TP1l = new Path("./Q7/TP1JAVA"),
                 TP2l = new Path("./Q7/TP2JAVA");

            fs.copyFromLocalFile( TP1l, TP1r);
            System.out.println("[i] file TP1JAVA copied ✅");

            fs.copyFromLocalFile( TP2l, TP2r);
            System.out.println("[i] file TP2JAVA copied ✅");

        }catch(Exception exc){
            System.err.println("!!O0ops Exception!! \n => "+exc.getMessage());
            exc.printStackTrace();
        }


    }

}
