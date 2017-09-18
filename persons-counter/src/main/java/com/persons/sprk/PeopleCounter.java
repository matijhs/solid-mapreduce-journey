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
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;
import java.util.regex.Pattern;


public class PeopleCounter {

    private static final Pattern SPACE = Pattern.compile(" ");


    /**
     * Schema class for persons data.
     */
    public static class Person implements Serializable {
        private String firstName;
        private String lastName;
        private String location;
        private int count;

        public Person(String firstName, String lastName, String location) {
            this.firstName = firstName;
            this.lastName = lastName;
            this.location = location;
            this.count = 1;
        }

        public String getFirstName() {
            return firstName;
        }

        public void setFirstName(String firstName) {
            this.firstName = firstName;
        }

        public String getLastName() {
            return lastName;
        }

        public void setLastName(String lastName) {
            this.lastName = lastName;
        }

        public String getLocation() {
            return location;
        }

        public void setLocation(String location) {
            this.location = location;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }
    }


    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage: PeopleCounter <file>");
            System.exit(1);
        }


        SparkContext spark = SparkContext.getOrCreate();
//        JavaRDD<String> data = spark.textFile(args[0], 1).toJavaRDD();


        // Load the text file into the RDD.
        JavaRDD<Person> linesRDD = spark.textFile(args[0], 1).toJavaRDD().map(
                new Function<String, Person>() {
                    public Person call(String line) throws Exception {

                        // split the line
                        // Person instance
                        // firstName lastName location
                        String[] fields = line.split(",");
                        return new Person(fields[0], fields[1], fields[2]);
                    }
                });


        //Mapping using a composite key: firstName,lastName,location
        JavaPairRDD<String, Integer> countedRDD =
                linesRDD.mapToPair(new PairFunction<Person, String, Integer>() {
                    public Tuple2<String, Integer> call(Person person) {
                        Tuple2<String, Integer> t2 = new Tuple2<>(
                                person.firstName + person.lastName + person.location, person.count);
                        return t2;
                    }
                });


        //Reduce records, key is firstName,lastName,location triplet, value is the count
        JavaPairRDD<String, Integer> final_rdd_records =
                countedRDD.reduceByKey(
                        new Function2<Integer, Integer, Integer>() {
                            @Override
                            public Integer call(Integer v1, Integer v2) throws Exception {
                                return v1 + v2;
                            }
                        });


        spark.stop();

        // HBASE
        Configuration conf = HBaseConfiguration.create();
        HBaseAdmin admin = new HBaseAdmin(conf);
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("people"));
        tableDescriptor.addFamily(new HColumnDescriptor("peopleCount"));
        admin.createTable(tableDescriptor);

        HTable table = new HTable(conf, "people");

        List<Tuple2<String, Integer>> l = final_rdd_records.collect();

        // load data into HBASE table
        for (Tuple2<String, Integer> rec : l) {
            String[] cells = rec._1.split(",");
            Put put = new Put(Bytes.toBytes(rec._1));
            put.addColumn(Bytes.toBytes("peopleCount"), Bytes.toBytes("firstName"), Bytes.toBytes(cells[0]));
            put.addColumn(Bytes.toBytes("peopleCount"), Bytes.toBytes("lastName"), Bytes.toBytes(cells[1]));
            put.addColumn(Bytes.toBytes("peopleCount"), Bytes.toBytes("location"), Bytes.toBytes(cells[2]));
            put.addColumn(Bytes.toBytes("peopleCount"), Bytes.toBytes("firstName"), Bytes.toBytes(rec._2));
            table.put(put);
        }

        table.flushCommits();
        table.close();

        //phoenix

//        CREATE TABLE "people"(
//                PK VARCHAR PRIMARY KEY,
//                "people"."value" VARCHAR);
//
//        sqlline> create view "people-count" (pk VARCHAR PRIMARY KEY, "data"."id" VARCHAR, "data"."categoryName" VARCHAR) as select * from "config";
//
//        CREATE TABLE "people_data" ( ROWKEY VARCHAR PRIMARY KEY, "firstName" VARCHAR, "lastName" VARCHAR, "location" VARCHAR, "count" VARCHAR) ;
//
//

    }

}
