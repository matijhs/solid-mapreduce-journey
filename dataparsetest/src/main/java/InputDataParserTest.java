import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Locale;
import java.util.Random;


public class InputDataParserTest {

    private static final String firstNames = "first-names.txt";
    private static final String lastNames = "names.txt";
    private static final String locations = "places.txt";
    private static final int numOfRecords = 100;
    private int createdRecords = 0;


    private Random rndm = new Random();
    private ArrayList<String> randomFirstNames = new ArrayList<String>();
    private ArrayList<String> randomLastNames = new ArrayList<String>();
    private ArrayList<String> randomBirthPlaces = new ArrayList<String>();
    private ArrayList<String> resultData = new ArrayList<String>();

    private SimpleDateFormat frmt = new SimpleDateFormat(
            "yyyy-MM-dd");

    public static void main(String[] args) {

        System.out.println("starting parser test...");
        InputDataParserTest idpt = new InputDataParserTest();


        try {
            BufferedReader firstNameRdr = new BufferedReader(new FileReader(firstNames));
            BufferedReader lastNameRdr = new BufferedReader(new FileReader(lastNames));
            BufferedReader locationRdr = new BufferedReader(new FileReader(locations));

            String line1;
            String line2;
            String line3;

            while ((line1 = firstNameRdr.readLine()) != null) {

                idpt.randomFirstNames.add(line1);
            }

            while ((line2 = lastNameRdr.readLine()) != null) {

                idpt.randomLastNames.add(line2);
            }

            while ((line3 = locationRdr.readLine()) != null) {

                idpt.randomBirthPlaces.add(line3);
            }

            firstNameRdr.close();
            lastNameRdr.close();
            locationRdr.close();

            if ((idpt.randomLastNames.size() == 0) || (idpt.randomFirstNames.size() == 0) || (idpt.randomBirthPlaces.size() == 0)) {
                throw new IOException("A random word list is empty: " + idpt.randomFirstNames.size()
                        + " " + idpt.randomLastNames.size() + " " + idpt.randomBirthPlaces.size());
            }

            idpt.printer();


        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


        System.out.println("creating test data...");

        while (idpt.createdRecords < numOfRecords) {

            String firstName = idpt.getRandomFirstName();
            String lastName = idpt.getRandomLastName();
            String birthPlace = idpt.getRandomPlace();

            long max =0L;
            long min =1000000000000L;

            String birthDate = idpt.frmt.format(new Date((idpt.rndm.nextLong() % (max - min)) + min));

            String randomRecord = firstName + "," + lastName + "," + birthPlace + "," + birthDate;
            idpt.resultData.add(randomRecord);
            ++idpt.createdRecords;
        }

        System.out.println("end of loop.");
        System.out.println(idpt.resultData);




    }


    public void printer() {
        System.out.println("printing arrays...");
        System.out.println(randomFirstNames);
        System.out.println(randomLastNames);
        System.out.println(randomBirthPlaces);
    }

    private String getRandomFirstName() {
        StringBuilder bldr = new StringBuilder();


            bldr.append(randomFirstNames.get(Math.abs(rndm.nextInt())
                    % randomFirstNames.size()));


        System.out.println("a rnd first name: " + bldr.toString());
        return bldr.toString();
    }

    private String getRandomPlace() {
        StringBuilder bldr = new StringBuilder();


        bldr.append(randomBirthPlaces.get(Math.abs(rndm.nextInt()) % randomBirthPlaces.size()));

        System.out.println("a rnd place: " + bldr.toString());
        return bldr.toString();
    }

    private String getRandomLastName() {
        StringBuilder bldr = new StringBuilder();

        bldr.append(randomLastNames.get(Math.abs(rndm.nextInt())
                    % randomLastNames.size()));

        System.out.println("a rnd last name: " + bldr.toString());
        return bldr.toString();
    }
}
