package Partie1;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

public class Q1 {

    public static void main(String[] args) {

        SparkSession ss=SparkSession.builder().
                appName("Q1) Afficher les projets en cours de réalisation").
                master("local[*]").getOrCreate();
        Map<String,String> options=new HashMap<>();
        options.put("driver","com.mysql.cj.jdbc.Driver");
        options.put("url","jdbc:mysql://192.168.67.1:3306/db_imomaroc");

        options.put("user","root");
        options.put("password","");


        System.out.println(" Q1) Afficher les projets en cours de réalisation ");
        Dataset<Row> ProjetsEnCours =ss.read().
                format("jdbc")
                .options(options)
                .option("query",
                        " Select * from projets p where p.date_fin>CURRENT_DATE   ")
                .load();
        ProjetsEnCours.show();





    }

}
