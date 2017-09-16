package com.persons.sprk;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class PeopleCounter {

    private static final Pattern SPACE = Pattern.compile(" ");


    public static class Person implements Serializable {
        private String firstName;
        private String lastName;
        private String location;

        //getters setters
    }


    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage: PeopleCounter <file>");
            System.exit(1);
        }

        SparkSession spark = SparkSession
                .builder()
                .appName("PeopleCounter")
                .getOrCreate();


        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());

        JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

        JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);

        List<Tuple2<String, Integer>> output = counts.collect();
        for (Tuple2<?, ?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        spark.stop();


        //HBASE
        Configuration conf = HBaseConfiguration.create();
        HBaseAdmin admin = new HBaseAdmin(conf);
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("people"));
        tableDescriptor.addFamily(new HColumnDescriptor("peopleLocationCount"));
        admin.createTable(tableDescriptor);


        HTable table = new HTable(conf, "people");
        Put put = new Put(Bytes.toBytes("doe-john-budapest-25"));
        put.add(Bytes.toBytes("peopleCount"), Bytes.toBytes("firstName"), Bytes.toBytes("John"));
        put.add(Bytes.toBytes("peopleCount"), Bytes.toBytes("lastName"), Bytes.toBytes("Doe"));
        put.add(Bytes.toBytes("peopleCount"), Bytes.toBytes("location"), Bytes.toBytes("Budapest"));
        put.add(Bytes.toBytes("peopleCount"), Bytes.toBytes("count"), Bytes.toBytes("25"));
        table.put(put);
        table.flushCommits();
        table.close();
    }
}
