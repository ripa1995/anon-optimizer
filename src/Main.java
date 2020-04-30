import anonymizer.Anonymizer;
import extractor.UtilityExtractor;
import org.deidentifier.arx.*;
import org.deidentifier.arx.criteria.KAnonymity;
import org.hsqldb.jdbc.JDBCStatement;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.stream.Stream;

public class Main {


    private static JDBCStatement statement;

    public static void main(String[] args) {
        UtilityExtractor utilityExtractor = new UtilityExtractor();
        utilityExtractor.runQueries(new String[]{"SELECT workclass,avg(age) from adult where workclass in ('private') group by workclass having avg(education_num) > 5","SELECT age from adult where salary_class='>50K'"});
        utilityExtractor.extractColumn();
        utilityExtractor.generateWorkloadUtility();
        Anonymizer anonymizer = new Anonymizer();
        try {
            anonymizer.init();
        } catch (IOException e) {
            e.printStackTrace();
        }
        anonymizer.optimize(utilityExtractor.getWorkloadUtility());
        anonymizer.anonymize();
        anonymizer.saveToCSV("./results/anonymized","adult_opt.csv");
    }

    private static void testUtilityExtractor(){
        UtilityExtractor utilityExtractor = new UtilityExtractor();
        utilityExtractor.runQueries(new String[]{"SELECT workclass,avg(age) from adult where workclass in ('private','public') and workclass not in ('gen') group by workclass having avg(education_num) > 5","SELECT age from adult where salary_class='>50K'"});
        utilityExtractor.extractColumn();
        System.out.println("Filter column: ");
        for (String s1:utilityExtractor.getFilterColumn()){
            System.out.println(s1);
        }
        System.out.println("Select column: ");
        for (String s1:utilityExtractor.getSelectColumn()){
            System.out.println(s1);
        }
        System.out.println("Group column: ");
        for (String s1:utilityExtractor.getGroupColumn()){
            System.out.println(s1);
        }
        System.out.println("Having column: ");
        for (String s1:utilityExtractor.getHavingColumn()){
            System.out.println(s1);
        }
        utilityExtractor.generateWorkloadUtility();
        System.out.println(utilityExtractor.getWorkloadUtilityResult());
    }


    private static void popolateTestDB(){
        try {
            Class.forName("org.hsqldb.jdbc.JDBCDriver" );
        } catch (Exception e) {
            System.err.println("ERROR: failed to load HSQLDB JDBC driver.");
            e.printStackTrace();
            return;
        }
        try {
            //choose database to use
            Connection c = DriverManager.getConnection("jdbc:hsqldb:file:data\\testdb", "SA", "");
            System.out.println("Connected");
            statement = (JDBCStatement) c.createStatement();
            importAdultDataset();
            c.close();
        } catch (IOException | SQLException e) {
            e.printStackTrace();
        }
    }

    private static void importAdultDataset() throws SQLException, IOException {
        statement.execute("drop table if exists adult");
        statement.execute("create table if not exists adult(age int, "
                + "workclass VARCHAR(50), " + "fnlwgt int, "
                + "education VARCHAR(50), " + "education_num int, "
                + "marital_status VARCHAR(50), " + "occupation VARCHAR(50), "
                + "relationship VARCHAR(50), " + "race VARCHAR(50), "
                + "sex VARCHAR(50), " + "capital_gain int, "
                + "capital_loss int, " + "hours_per_week int, "
                + "native_country VARCHAR(50), " + "salary_class VARCHAR(50))");

        ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) FROM adult");
        resultSet.next();
        if (resultSet.getInt(1)==0) {
            Stream st = Files.lines(Paths.get("./data/dataset/adult_clear.csv")).skip(1);
            st.forEach(Main::addAdult);
        }
    }

    private static void addAdult(Object o) {
        String s = (String) o;
        s = s.replaceAll("\"", "'");
        String[] values = s.split(",");
        try {
            statement.execute("insert into adult values(" +
                    values[0] + "," + values[1] + "," +
                    values[2] + "," + values[3] + "," +
                    values[4] + "," + values[5] + "," +
                    values[6] + "," + values[7] + "," +
                    values[8] + "," + values[9] + "," +
                    values[10] + "," + values[11] + "," +
                    values[12] + "," + values[13] + "," +
                    values[14] + ")");
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

}
