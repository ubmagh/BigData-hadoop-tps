package Partie2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

public class Q3 {


    public static void main(String[] args) {
        SparkSession ss = SparkSession.builder().appName("Q3) Afficher le nombre d’appartements vendues à Casablanca.")
                .master("local[*]").getOrCreate();

        Dataset<Row> df = ss.read().option("multiline", true).json("achats.json");

        System.out.println("Q3) Afficher le nombre d’appartements vendues à Casablanca.");
        df.select(
                col("adresse")
        ).where( col("adresse").equalTo("casablanca") ) // c'est normalement la ville :/
                .groupBy( col("adresse") ).count().show();

    }

}
