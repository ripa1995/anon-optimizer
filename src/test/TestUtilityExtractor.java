package test;

import extractor.UtilityExtractor;
import junit.framework.TestCase;

import java.sql.*;

public class TestUtilityExtractor extends TestCase {

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

    //------------------------------------------------------------
    // Constructors
    //------------------------------------------------------------

    /**
     * Constructs a new SubselectTest.
     */
    public TestUtilityExtractor(String s) {
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

        if (conn != null && utilityExtractor!=null) {
            return;
        }

        Class.forName(databaseDriver);

        conn = getJDBCConnection();
        stmt = conn.createStatement();

        try {
            stmt.execute("drop table employee if exists");
        } catch (Exception x) {}

        stmt.execute("create table employee(id int, "
                + "firstname VARCHAR(50), " + "lastname VARCHAR(50), "
                + "salary decimal(10, 2), " + "superior_id int, "
                + "CONSTRAINT PK_employee PRIMARY KEY (id), "
                + "CONSTRAINT FK_superior FOREIGN KEY (superior_id) "
                + "REFERENCES employee(ID))");
        addEmployee(1, "Mike", "Smith", 160000, -1);
        addEmployee(2, "Mary", "Smith", 140000, -1);

        // Employee under Mike
        addEmployee(10, "Joe", "Divis", 50000, 1);
        addEmployee(11, "Peter", "Mason", 45000, 1);
        addEmployee(12, "Steve", "Johnson", 40000, 1);
        addEmployee(13, "Jim", "Hood", 35000, 1);

        // Employee under Mike
        addEmployee(20, "Jennifer", "Divis", 60000, 2);
        addEmployee(21, "Helen", "Mason", 50000, 2);
        addEmployee(22, "Daisy", "Johnson", 40000, 2);
        addEmployee(23, "Barbara", "Hood", 30000, 2);

        stmt.execute("drop table colors if exists; "
                + "drop table sizes if exists; ");
        stmt.execute("create table colors(id int, val varchar(10)); ");
        stmt.execute("insert into colors values(1,'red'); "
                + "insert into colors values(2,'green'); "
                + "insert into colors values(3,'orange'); "
                + "insert into colors values(4,'indigo'); ");
        stmt.execute("create table sizes(id int, val varchar(10)); ");
        stmt.execute("insert into sizes values(1,'small'); "
                + "insert into sizes values(2,'medium'); "
                + "insert into sizes values(3,'large'); "
                + "insert into sizes values(4,'odd'); ");

        utilityExtractor = new UtilityExtractor(databaseURL,databaseUser,databasePassword);
    }

    protected void tearDown() throws Exception {

        try {
            stmt.execute("drop table employee if exists; "+"drop table colors if exists; "
                    + "drop table sizes if exists; ");
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

        super.tearDown();

    }

    private void addEmployee(int id, String firstName, String lastName,
                             double salary, int superiorId) throws Exception {

        stmt.execute("insert into employee values(" + id + ", '" + firstName
                + "', '" + lastName + "', " + salary + ", "
                + (superiorId <= 0 ? "null"
                : ("" + superiorId)) + ")");
    }

    /**
     * Test simple & single workload extraction
     */
    public void testSimpleSingleWorkload() throws SQLException {

        String[] queries = new String[]{"select firstName from employee;"};
        String expected = "WORKLOAD:\n" +
                "-COLUMN: FIRSTNAME";

        compareResults(queries, expected);
    }

    /**
     * Test simple multiple workload extraction
     */
    public void testSimpleMultipleWorkload() throws SQLException {

        String[] queries = new String[]{"select firstName from employee;","select salary from employee;"};
        String expected = "WORKLOAD:\n" +
                "-COLUMN: SALARY\n" +
                "-COLUMN: FIRSTNAME";

        compareResults(queries, expected);
    }

    /**
     * Test single workload extraction with filter
     */
    public void testWorkloadWithFilter() throws SQLException {

        String[] queries = new String[]{"select firstName from employee where id=20;"};
        String expected = "WORKLOAD:\n" +
                "-COLUMN: FIRSTNAME\n" +
                "-COLUMN: ID\n" +
                "--TYPE: LOGIC.EQUAL\n" +
                "---VALUE: 20";

        compareResults(queries, expected);
    }

    /**
     * Test complete workload extraction with agg, group by and having
     */
    public void testCompleteWorkload() throws SQLException {

        String[] queries = new String[]{"select firstName,max(salary) from employee where id=20 group by firstName;","select id,avg(salary) from employee group by id having (max(salary) > 100);",};
        String expected = "WORKLOAD:\n" +
                "-COLUMN: SALARY\n" +
                "--TYPE: AGGTYPE.AVG\n" +
                "--TYPE: LOGIC.GREATER\n" +
                "---VALUE: 100\n" +
                "--TYPE: AGGTYPE.MAX\n" +
                "-COLUMN: FIRSTNAME\n" +
                "-COLUMN: ID\n" +
                "--TYPE: LOGIC.EQUAL\n" +
                "---VALUE: 20";

        compareResults(queries, expected);
    }

    //------------------------------------------------------------
    // Helper methods
    //------------------------------------------------------------
    private void compareResults(String[] queries, String row){

        int result = utilityExtractor.runQueries(queries);
        if (result>0) {
            result=0;
        }
        assertEquals("Failed to run queries.",0,result);
        utilityExtractor.extractColumn();
        utilityExtractor.generateWorkloadUtility();

        String r = utilityExtractor.getWorkloadUtilityResult();

        assertEquals("Failed to generate workload.", row,
                r);
    }
}
