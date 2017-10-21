import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;

/**
 * Created by kris on 10/20/2017.
 */
public class Main{
    public static void main(String[] args) throws SQLException{

        // load postgres db properties from postgres.properties
        Properties aws_props = new Properties();
        try {
            aws_props.load(Main.class.getClassLoader().getResourceAsStream("s3.properties"));

        } catch (IOException ex) {
            ex.printStackTrace();
        }

        String awsAccessKey = aws_props.getProperty("awsAccessKey");
        String awsSecretKey = aws_props.getProperty("awsSecretKey");
        String s3bucket = aws_props.getProperty("s3bucket");

        BasicAWSCredentials awsCreds = new BasicAWSCredentials(awsAccessKey, awsSecretKey);
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                .build();



        // load postgres db properties from postgres.properties
        Properties pg_props = new Properties();
        try {
            pg_props.load(Main.class.getClassLoader().getResourceAsStream("postgres.properties"));

        } catch (IOException ex) {
            ex.printStackTrace();
        }

        String DB_DRIVER = pg_props.getProperty("DB_DRIVER");
        String DB_CONNECTION = pg_props.getProperty("DB_CONNECTION");
        String DB_USER = pg_props.getProperty("DB_USER");
        String DB_PASSWORD = pg_props.getProperty("DB_PASSWORD");
        // load jdbc driver
        try {
            Class.forName(DB_DRIVER);

        } catch (ClassNotFoundException e) {
            System.out.println(e.getMessage());
        }

        // initialize dbconnection
        Connection dbConnection = null;

        try {
            dbConnection = DriverManager.getConnection(DB_CONNECTION, DB_USER,
                    DB_PASSWORD);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

        // select all tracks where rejected = true
        Statement statement = null;

        String selectRejectedTracksSQL = "SELECT * from TRACKS WHERE rejected = TRUE";

        ArrayList<String> tracksToDelete = new ArrayList<String>();

        try {
            statement = dbConnection.createStatement();

            System.out.println(selectRejectedTracksSQL);

            // execute select SQL statement
            ResultSet rs = statement.executeQuery(selectRejectedTracksSQL);

            while (rs.next()) {

                String s3key = rs.getString("s3key");
                tracksToDelete.add(s3key);

            }

        } catch (SQLException e) {

            System.out.println(e.getMessage());

        }


        statement = null;

        String deleteTracksSQL = "DELETE from TRACKS WHERE rejected = TRUE";

        try {
            statement = dbConnection.createStatement();

            System.out.println(deleteTracksSQL);

            // execute delete SQL statement
            ResultSet rs = statement.executeQuery(deleteTracksSQL);

        } catch (SQLException e) {

            System.out.println(e.getMessage());

        } finally {

            if (statement != null) {
                statement.close();
            }

            if (dbConnection != null) {
                dbConnection.close();
            }

        }

        for(String key : tracksToDelete){
            System.out.println("Deleting from S3: " + key);

            s3Client.deleteObject(s3bucket, key);
        };

    }

}
