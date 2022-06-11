package Partie2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.count;

public class Q2 {


    public static void main(String[] args) {
        SparkSession ss = SparkSession.builder().appName("Q2) Afficher la liste des clients qui possèdent plus qu’un appartement à rabat.")
                .master("local[*]").getOrCreate();

        Dataset<Row> df = ss.read().option("multiline", true).json("achats.json");

        System.out.println("Q2) Afficher la liste des clients qui possèdent plus qu’un appartement à rabat.");
        df.select(
                col("numC"),
                col("nom"),
                col("prenom"),
                col("adresse")
        ).where( col("adresse").equalTo("rabat") ) // c'est normalement la ville :/
                .groupBy( col("numC"), col("nom"), col("prenom") ).count().where(col("count").gt(1)).show();

    }

}
