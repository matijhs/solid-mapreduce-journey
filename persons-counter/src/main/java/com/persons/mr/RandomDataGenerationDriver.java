package com.persons.mr;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.*;
import java.security.InvalidParameterException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.logging.Logger;

public class RandomDataGenerationDriver {

    public static class RandomInputFormat extends
            InputFormat<Text, NullWritable> {

        public static final String NUM_MAP_TASKS = "random.generator.map.tasks";
        public static final String NUM_RECORDS_PER_TASK = "random.generator.num.records.per.map.task";
        public static final String RANDOM_WORD_LIST = "random.generator.random.word.file";

        @Override
        public List<InputSplit> getSplits(JobContext job) throws IOException {

            int numSplits = job.getConfiguration().getInt(NUM_MAP_TASKS, -1);
            if (numSplits <= 0) {
                throw new IOException(NUM_MAP_TASKS + " is not set.");
            }

            // Create a number of input splits equivalent to the number of tasks
            ArrayList<InputSplit> splits = new ArrayList<InputSplit>();
            for (int i = 0; i < numSplits; ++i) {
                splits.add(new FakeInputSplit());
            }

            return splits;
        }

        @Override
        public RecordReader<Text, NullWritable> createRecordReader(
                InputSplit split, TaskAttemptContext context)
                throws IOException, InterruptedException {
            // Create a new RandomRecordReader and initialize it
            RandomRecordReader rr = new RandomRecordReader();
            rr.initialize(split, context);

            return rr;
        }

        public static void setNumMapTasks(Job job, int i) {
            job.getConfiguration().setInt(NUM_MAP_TASKS, i);
        }

        public static void setNumRecordPerTask(Job job, int i) {
            job.getConfiguration().setInt(NUM_RECORDS_PER_TASK, i);
        }

        public static void setRandomWordList(Job job, String[] files) {
            job.getConfiguration().setStrings("input.files", files);
        }

        public static class RandomRecordReader extends
                RecordReader<Text, NullWritable> {

            Logger logger = Logger.getLogger("RecordReader");
            private int numRecordsToCreate = 0;
            private int createdRecords = 0;
            private Text key = new Text();
            private NullWritable value = NullWritable.get();
            private Random rndm = new Random();
            private ArrayList<String> randomFirstNames = new ArrayList<String>();
            private ArrayList<String> randomLastNames = new ArrayList<String>();
            private ArrayList<String> randomBirthPlaces = new ArrayList<String>();

            // This object will format the creation date string into a Date
            // object
            private SimpleDateFormat frmt = new SimpleDateFormat(
                    "yyyy-MM-dd");


            @Override
            public void initialize(InputSplit split, TaskAttemptContext context)
                    throws IOException, InterruptedException {

                // Get the number of records to create from the configuration
                this.numRecordsToCreate = context.getConfiguration().getInt(
                        NUM_RECORDS_PER_TASK, -1);

                if (numRecordsToCreate < 0) {
                    throw new InvalidParameterException(NUM_RECORDS_PER_TASK
                            + " is not set.");
                }


                String[] files = context.getConfiguration().getStrings("input.files");
                if (files.length == 0) {
                    throw new InvalidParameterException(
                            "Random word list not set in cache.");
                } else {
                    // Read the list of random words into a list

                    FileSystem fs = FileSystem.get(context.getConfiguration());
                    BufferedReader firstNameRdr = new BufferedReader(new InputStreamReader(fs.open(new Path(files[0]))));
                    BufferedReader lastNameRdr = new BufferedReader(new InputStreamReader(fs.open(new Path(files[1]))));
                    BufferedReader birthPlaceRdr = new BufferedReader(new InputStreamReader(fs.open(new Path(files[2]))));

                    String line1;
                    String line2;
                    String line3;


                    while ((line1 = firstNameRdr.readLine()) != null) {

                        randomFirstNames.add(line1);
                    }

                    while ((line2 = lastNameRdr.readLine()) != null) {

                        randomLastNames.add(line2);
                    }

                    while ((line3 = birthPlaceRdr.readLine()) != null) {

                        randomBirthPlaces.add(line3);
                    }

                    firstNameRdr.close();
                    lastNameRdr.close();
                    birthPlaceRdr.close();

                    if ((randomLastNames.size() == 0) || (randomFirstNames.size() == 0) || (randomBirthPlaces.size() == 0)) {
                        throw new IOException("A random word list is empty: " + randomFirstNames.size()
                                + " " + randomLastNames.size() + " " + randomBirthPlaces.size());
                    }

                }
            }

            public void printer() {
                System.out.println(randomFirstNames);
                System.out.println(randomLastNames);
                System.out.println(randomBirthPlaces);
            }

            @Override
            public boolean nextKeyValue() throws IOException,
                    InterruptedException {
                // If we still have records to create
                if (createdRecords < numRecordsToCreate) {
                    // Generate random data
                    // todo

                    // Create a string of text from the random words
                    String firstName = getRandomFirstName();
                    String lastName = getRandomLastName();
                    String birthPlace = getRandomPlace();

                    long max =0L;
                    long min =1000000000000L;
                    String birthDate = frmt.format(new Date((rndm.nextLong() % (max - min)) + min));

                    String randomRecord = firstName + "," + lastName + "," + birthPlace + "," + birthDate;

                    key.set(randomRecord);
                    ++createdRecords;
                    return true;
                } else {
                    // Else, return false
                    return false;
                }
            }

            /**
             * Creates a random string of words from the list. 1-30 words per
             * string.
             *
             * @return A random string of words
             */
            private String getRandomFirstName() {
                StringBuilder bldr = new StringBuilder();

                bldr.append(randomFirstNames.get(Math.abs(rndm.nextInt())
                        % randomFirstNames.size()));

                return bldr.toString();
            }

            private String getRandomPlace() {
                StringBuilder bldr = new StringBuilder();

                bldr.append(randomBirthPlaces.get(Math.abs(rndm.nextInt())
                        % randomBirthPlaces.size()));

                return bldr.toString();
            }

            private String getRandomLastName() {
                StringBuilder bldr = new StringBuilder();

                bldr.append(randomLastNames.get(Math.abs(rndm.nextInt())
                        % randomLastNames.size()));

                return bldr.toString();
            }

            @Override
            public Text getCurrentKey() throws IOException,
                    InterruptedException {
                return key;
            }

            @Override
            public NullWritable getCurrentValue() throws IOException,
                    InterruptedException {
                return value;
            }

            @Override
            public float getProgress() throws IOException, InterruptedException {
                return (float) createdRecords / (float) numRecordsToCreate;
            }

            @Override
            public void close() throws IOException {
                // nothing to do here...
            }
        }

        /**
         * This class is very empty.
         */
        public static class FakeInputSplit extends InputSplit implements
                Writable {


            public void readFields(DataInput arg0) throws IOException {
            }


            public void write(DataOutput arg0) throws IOException {
            }


            public long getLength() throws IOException, InterruptedException {
                return 0;
            }


            public String[] getLocations() throws IOException,
                    InterruptedException {
                return new String[0];
            }
        }
    }

    /**
     * Driver
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
//        conf.set("mapred.job.tracker", "localhost:8888");
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();

        FileSystem hdfs = FileSystem.get(conf);
        System.out.println("cwd: " + hdfs.getWorkingDirectory().toString());
        String cwd = hdfs.getWorkingDirectory().toString() + "/";
        Path out = new Path(cwd + args[4]);
        System.out.println("OUTPUT file: " + out.toString());
        if (hdfs.isDirectory(out) || hdfs.isFile(out)) {
            System.out.println("cleaning up output dir...");
            hdfs.delete(out, true);
        }


        if (otherArgs.length != 5) {
            System.err
                    .println("Usage: RandomDataGenerationDriver <num records> <first name list> <last name list> <location list> <output>");
            System.exit(1);
        }

        // hardcoded number of tasks
        final int numMapTasks = 1;

        // number of records to create
        int numRecordsPerTask = Integer.parseInt(otherArgs[0]);
        Path firstNames = new Path(otherArgs[1]);
        Path lastNames = new Path(otherArgs[2]);
        Path locations = new Path(otherArgs[3]);
        Path outputDir = new Path(otherArgs[4]);

        Job job = new Job(conf, "RandomDataGenerator");
        job.setJarByClass(RandomDataGenerationDriver.class);

        job.setNumReduceTasks(0);

        job.setInputFormatClass(RandomInputFormat.class);

        RandomInputFormat.setNumMapTasks(job, numMapTasks);
        RandomInputFormat.setNumRecordPerTask(job, numRecordsPerTask);

        RandomInputFormat.setRandomWordList(job, new String[]{cwd + firstNames.toString(), cwd + lastNames.toString(), cwd + locations.toString()});

        TextOutputFormat.setOutputPath(job, outputDir);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 2);


    }
}