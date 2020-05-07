package test;

import anonymizer.Anonymizer;
import extractor.UtilityExtractor;
import junit.framework.Test;
import junit.framework.TestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.stream.Stream;


public class TestAnonymizer extends TestCase {


    //------------------------------------------------------------
    // Class variables
    //------------------------------------------------------------
    private static final String databaseDriver   = "org.hsqldb.jdbc.JDBCDriver";
    private static final String databaseURL      = "jdbc:hsqldb:mem:.";
    private static final String databaseUser     = "sa";
    private static final String databasePassword = "";


    //------------------------------------------------------------
    // Instance variables
    //------------------------------------------------------------
    private Connection conn;
    private Statement stmt;
    private UtilityExtractor utilityExtractor;
    private Anonymizer anonymizer;

    //------------------------------------------------------------
    // Constructors
    //------------------------------------------------------------

    /**
     * Constructs a new SubselectTest.
     */
    public TestAnonymizer(String s) {
        super(s);
    }

    //------------------------------------------------------------
    // Class methods
    //------------------------------------------------------------
    protected static Connection getJDBCConnection() throws SQLException {
        return DriverManager.getConnection(databaseURL, databaseUser,
                databasePassword);
    }

    protected void setUp() throws Exception {

        super.setUp();

        if (conn != null && utilityExtractor!=null && anonymizer!=null) {
            return;
        }

        Class.forName(databaseDriver);

        conn = getJDBCConnection();
        stmt = conn.createStatement();
        importAdultDataset();
        utilityExtractor = new UtilityExtractor(databaseURL,databaseUser,databasePassword);
        anonymizer = new Anonymizer(".\\data\\dataset\\adult_clear.csv",new String[]{},new String[]{},
                new String[]{"fnlwgt","education-num","relationship","capital-gain","capital-loss","hours-per-week"},
                new String[]{"age","workclass","education","marital-status","occupation","race","sex","native-country","salary-class"},
                new String[]{".\\data\\hierarchies\\adult_age.csv",
                        ".\\data\\hierarchies\\adult_workclass.csv",
                        ".\\data\\hierarchies\\adult_education.csv",
                        ".\\data\\hierarchies\\adult_marital-status.csv",
                        ".\\data\\hierarchies\\adult_occupation.csv",
                        ".\\data\\hierarchies\\adult_race.csv",
                        ".\\data\\hierarchies\\adult_sex.csv",
                        ".\\data\\hierarchies\\adult_native-country.csv",
                        ".\\data\\hierarchies\\adult_salary-class.csv"},
                ',',
                ';');
    }

    protected void tearDown() throws Exception {

        try {
            stmt.execute("drop table adult if exists; ");
        } catch (Exception x) {}

        if (stmt != null) {
            stmt.close();

            stmt = null;
        }

        if (conn != null) {
            conn.close();

            conn = null;
        }
        if (utilityExtractor!=null){
            utilityExtractor=null;
        }
        if(anonymizer!=null){
            anonymizer=null;
        }

        super.tearDown();

    }

    private void importAdultDataset() throws SQLException, IOException {
        stmt.execute("drop table if exists adult");
        stmt.execute("create table if not exists adult(age int, "
                + "workclass VARCHAR(50), " + "fnlwgt int, "
                + "education VARCHAR(50), " + "education_num int, "
                + "marital_status VARCHAR(50), " + "occupation VARCHAR(50), "
                + "relationship VARCHAR(50), " + "race VARCHAR(50), "
                + "sex VARCHAR(50), " + "capital_gain int, "
                + "capital_loss int, " + "hours_per_week int, "
                + "native_country VARCHAR(50), " + "salary_class VARCHAR(50))");

        ResultSet resultSet = stmt.executeQuery("SELECT COUNT(*) FROM adult");
        resultSet.next();
        if (resultSet.getInt(1)==0) {
            Stream st = Files.lines(Paths.get("./data/dataset/adult_clear.csv")).skip(1);
            st.forEach(o -> addAdult(o));
        }
    }

    private void addAdult(Object o) {
        String s = (String) o;
        s = s.replaceAll("\"", "'");
        String[] values = s.split(",");
        try {
            stmt.execute("insert into adult values(" +
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

    /**
     * Test simple workload anonymization
     */
    public void testSimpleWorkloadAnonymization() {
        utilityExtractor.runQueries(new String[]{"SELECT education from adult where workclass in ('private')"});
        utilityExtractor.extractColumn();
        utilityExtractor.generateWorkloadUtility();
        anonymizer.setKAnonymity(5);
        anonymizer.setSuppressionLimit(0.04);
        anonymizer.optimizedAnonymization(utilityExtractor.getWorkloadUtility());
        String result = anonymizer.getGeneralization();
        String expected = "0/3,2/2,0/2,1/1,1/1,2/2,1/1,2/2,4/4";

        compareResults(result, expected);
    }

    /**
     * Test hard workload anonymization
     */
    public void testHardWorkloadAnonymization() {
        utilityExtractor.runQueries(new String[]{"SELECT education,age,salary_class,marital_status from adult where workclass in ('private')"});
        utilityExtractor.extractColumn();
        utilityExtractor.generateWorkloadUtility();
        anonymizer.setKAnonymity(5);
        anonymizer.setSuppressionLimit(0.04);
        anonymizer.optimizedAnonymization(utilityExtractor.getWorkloadUtility());
        String result = anonymizer.getGeneralization();
        String expected = "1/3,2/2,0/2,0/1,1/1,2/2,1/1,0/2,2/4";

        compareResults(result, expected);
    }

    //------------------------------------------------------------
    // Helper methods
    //------------------------------------------------------------
    private void compareResults(String result, String row){

        int supp = 0;
        if(result==null){
            supp = -1;
        }
        assertEquals("Impossible to anonymize with this workload.", 0, supp);

        assertEquals("Wrong Optimization", row, result);
    }
}
