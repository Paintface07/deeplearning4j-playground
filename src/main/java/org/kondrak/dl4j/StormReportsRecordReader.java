package org.kondrak.dl4j;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.datavec.api.transform.TransformProcess;
import org.datavec.api.transform.schema.Schema;

import java.time.LocalDateTime;

public class StormReportsRecordReader {

    public static void main(String[] args) throws Exception {
        int skipLines = 0;
        String delimiter = ",";

        String baseDir = "./";
        String fileName = "reports.csv";
        String inputPath = String.format("%s%s", baseDir, fileName);
        String timestamp = String.valueOf(LocalDateTime.now());

        Schema inputDataSchema = new Schema.Builder()
                .addColumnsString("datetime", "severity", "location", "county", "state")
                .addColumnsDouble("lat", "lon")
                .addColumnsString("comment")
                .addColumnCategorical("type", "TOR", "WIND", "HAIL")
                .build();

        TransformProcess tp = new TransformProcess.Builder(inputDataSchema)
                .removeColumns("datetime", "severity", "location", "county", "state", "comment")
                .categoricalToInteger("type")
                .build();

        int numActions = tp.getActionList().size();
        for(int i = 0; i < numActions; i++) {
            System.out.println("\n\n=======================");
            System.out.println("--- Schema after step " + i + " (" + tp.getActionList().get(i) + ")--");
            System.out.println(tp.getSchemaAfterStep(i));
        }

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        sparkConf.setAppName("Storm Reports Record Reader Transform");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

//        https://www.youtube.com/watch?v=MLEMw2NxjxE&t=14s
//
//        JavaRDD<String> lines = sc.textFile(inputPath);
//        JavaRDD<List<Writable>> stormReports = lines.map(new StringToWritablesFunction());
//        SparkTransformExecutor.execute(stormReports);
    }
}
