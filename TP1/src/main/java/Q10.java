import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.net.URI;

public class Q10 {
    /*
        Display recursively /BDCC directory contents
     */
    public static void main(String[] args) {
        Configuration conf = ConnectionToHDFS.getConf();
        try{
            FileSystem fs = FileSystem.get( URI.create(ConnectionToHDFS.getUrl()), conf);
            Path path=new Path("/BDCC");
            RemoteIterator<LocatedFileStatus> it = fs.listFiles(path,true);
            while (it.hasNext()){
                LocatedFileStatus lfs=it.next();
                System.out.println( "File : '"+lfs.getPath().toString()+"' blockSize: "+lfs.getBlockSize()+"Bytes  Replications: "+ lfs.getReplication());
            }
            fs.close();
        }catch(Exception exc){
            System.err.println("!!O0ops Exception!! \n => "+exc.getMessage());
            exc.printStackTrace();
        }


    }

}
