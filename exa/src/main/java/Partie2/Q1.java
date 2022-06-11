package Partie2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.desc;

public class Q1 {


    public static void main(String[] args) {
        SparkSession ss = SparkSession.builder().appName("Q1) Afficher les deux première villes ou l’entreprise a réalisé plus de ventes (en terme de revenues)")
                .master("local[*]").getOrCreate();

        Dataset<Row> df = ss.read().option("multiline", true).json("achats.json");

        System.out.println("Q1) Afficher les deux première villes ou l’entreprise a réalisé plus de ventes (en terme de revenues)");
        df.select(
                col("ville"),
                col("prix")
        ).groupBy( col("ville")).sum("prix").orderBy(desc("sum(prix)")).limit(2).show();

    }

}
