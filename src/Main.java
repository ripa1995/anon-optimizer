import anonymizer.Anonymizer;
import extractor.UtilityExtractor;
import org.hsqldb.jdbc.JDBCStatement;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Scanner;
import java.util.stream.Stream;

public class Main {


    private static JDBCStatement statement;
    private static String datasetPath, saveDir, saveFileName, db_url, db_username, db_password;
    private static char datasetDel, hierarchyDel;
    private static String[] queries, identifyingAtt, sensitiveAtt, insensitiveAtt, qidAtt, qidHierPath, orderedDistanceTClosenessAtt, equalDistanceTClosenessAtt, entropyLDiversityAtt,  distinctLDiversityAtt,  recursiveCLDiversityAtt;
    private static double[] orderedDistanceTClosenessValue, equalDistanceTClosenessValue, entropyLDiversityValue, recursiveCLDiversityCValue;
    private static int[] distinctLDiversityValue, recursiveCLDiversityLValue;
    private static int k;
    private static double suppression;
    private static UtilityExtractor utilityExtractor;
    private static Anonymizer anonymizer;

    public static void main(String[] args) {
        System.out.println("Welcome to Anon-Optimizer!"+"\n"+"Before starting, please be sure to have a correct " +
                "HSQL db set up with your required dataset table, with column names equal to the one inside " +
                "the .csv dataset that you want to anonymize.");
        System.out.println("Also remember to set up your config.json file inside the config folder.");
        File file = new File("./config/config.json");
        //Extract information from config file
        extractConfigs(file);
        //Get info about db
        Scanner in = new Scanner(System.in);
        System.out.println("Please insert the HSQL database url (example: jdbc:hsqldb:file:<path to db>");
        db_url = in.nextLine();
        if (db_url.length()==0){
            db_url = "jdbc:hsqldb:file:data\\testdb";
        }
        System.out.println("Please insert username (default: SA)");
        db_username = in.nextLine();
        System.out.println("Please insert password (default: no password)");
        db_password = in.nextLine();
        //Initialize the utility extractor and generate the workload
        initUtilityExtractor();
        //Print the workload
        System.out.println(utilityExtractor.getWorkloadUtilityResult());
        //Initialize the anonymizer and set all privacy policies
        initAnonymizer();
        //Run the anonymizer in it's optimized version
        anonymizer.optimizedAnonymization(utilityExtractor.getWorkloadUtility());
        //Save the dataset anonymized and statistics
        anonymizer.saveToCSV(saveDir,saveFileName);
    }

    private static void initAnonymizer() {
        anonymizer = new Anonymizer(datasetPath,identifyingAtt,sensitiveAtt,insensitiveAtt,qidAtt,qidHierPath,datasetDel,hierarchyDel);
        anonymizer.setKAnonymity(k);
        anonymizer.setSuppressionLimit(suppression);
        int sensitiveAttNumber = sensitiveAtt.length;
        if (sensitiveAttNumber>0){
            if(orderedDistanceTClosenessAtt.length>0){
                if (orderedDistanceTClosenessValue.length==orderedDistanceTClosenessAtt.length){
                    for (int i=0; i<orderedDistanceTClosenessAtt.length;i++){
                        anonymizer.setOrderedDistanceTCloseness(orderedDistanceTClosenessAtt[i],orderedDistanceTClosenessValue[i]);
                    }
                } else {
                    System.err.println("Please check configs, there's a mismatch between Ordered Distance T Closeness Attribute and T Value!");
                }
            }
            if(equalDistanceTClosenessAtt.length>0){
                if (equalDistanceTClosenessValue.length==equalDistanceTClosenessAtt.length){
                    for (int i=0; i<equalDistanceTClosenessAtt.length;i++) {
                        anonymizer.setEqualDistanceTCloseness(equalDistanceTClosenessAtt[i], equalDistanceTClosenessValue[i]);
                    }
                } else {
                    System.err.println("Please check configs, there's a mismatch between Equal Distance T Closeness Attribute and T Value!");
                }
            }
            if(entropyLDiversityAtt.length>0){
                if (entropyLDiversityValue.length==entropyLDiversityAtt.length){
                    for (int i=0; i<entropyLDiversityAtt.length;i++) {
                        anonymizer.setEntropyLDiversity(entropyLDiversityAtt[i], entropyLDiversityValue[i]);
                    }
                } else {
                    System.err.println("Please check configs, there's a mismatch between Entropy L Diversity Attribute and L Value!");
                }
            }
            if(distinctLDiversityAtt.length>0){
                if (distinctLDiversityValue.length==distinctLDiversityAtt.length){
                    for (int i=0; i<distinctLDiversityAtt.length;i++) {
                        anonymizer.setDistinctLDiversity(distinctLDiversityAtt[i], distinctLDiversityValue[i]);
                    }
                } else {
                    System.err.println("Please check configs, there's a mismatch between Distinct L Diversity Attribute and L Value!");
                }
            }
            if(recursiveCLDiversityAtt.length>0){
                if (recursiveCLDiversityLValue.length==recursiveCLDiversityCValue.length && recursiveCLDiversityLValue.length==recursiveCLDiversityAtt.length){
                    for (int i=0; i<recursiveCLDiversityAtt.length;i++) {
                        anonymizer.setRecursiveCLDiversity(recursiveCLDiversityAtt[i], recursiveCLDiversityCValue[i],recursiveCLDiversityLValue[i]);
                    }
                } else {
                    System.err.println("Please check configs, there's a mismatch between Recursive C L Diversity Attribute, L Value and C Value!");
                }
            }
            if(sensitiveAttNumber>(orderedDistanceTClosenessAtt.length+equalDistanceTClosenessAtt.length+entropyLDiversityAtt.length+distinctLDiversityAtt.length+recursiveCLDiversityAtt.length)){
                System.err.println("Please check configs, all sensitive attribute must have their privacy policy [T-closeness, L-diversity, C-L-diversity]");
            }
        }
    }

    private static void initUtilityExtractor() {
        utilityExtractor = new UtilityExtractor(db_url,db_username,db_password);
        utilityExtractor.runQueries(queries);
        utilityExtractor.extractColumn();
        utilityExtractor.generateWorkloadUtility();
    }

    private static void extractConfigs(File json){
        JSONParser jsonParser = new JSONParser();
        try {
            Object object = jsonParser.parse(new FileReader(json));
            JSONObject jsonObject = (JSONObject) object;

            JSONArray jsonArray = (JSONArray)jsonObject.get("QUERY");
            queries = new String[jsonArray.size()];
            for(int i=0;i<jsonArray.size();i++){
                queries[i]= (String) jsonArray.get(i);
            }

            datasetPath = (String) jsonObject.get("DATASETPATH");

            jsonArray = (JSONArray)jsonObject.get("IDENTIFYINGATT");
            identifyingAtt = new String[jsonArray.size()];
            for(int i=0;i<jsonArray.size();i++){
                identifyingAtt[i]= (String) jsonArray.get(i);
            }

            jsonArray = (JSONArray)jsonObject.get("SENSITIVEATT");
            sensitiveAtt = new String[jsonArray.size()];
            for(int i=0;i<jsonArray.size();i++){
                sensitiveAtt[i]= (String) jsonArray.get(i);
            }

            jsonArray = (JSONArray)jsonObject.get("INSENSITIVEATT");
            insensitiveAtt = new String[jsonArray.size()];
            for(int i=0;i<jsonArray.size();i++){
                insensitiveAtt[i]= (String) jsonArray.get(i);
            }

            jsonArray = (JSONArray)jsonObject.get("QIDATT");
            qidAtt = new String[jsonArray.size()];
            for(int i=0;i<jsonArray.size();i++){
                qidAtt[i]= (String) jsonArray.get(i);
            }

            jsonArray = (JSONArray)jsonObject.get("QIDHIERPATH");
            qidHierPath = new String[jsonArray.size()];
            for(int i=0;i<jsonArray.size();i++){
                qidHierPath[i]= (String) jsonArray.get(i);
            }

            datasetDel = ((String) jsonObject.get("DATASETDELIMITER")).charAt(0);
            hierarchyDel = ((String) jsonObject.get("HIERARCHYDELIMITER")).charAt(0);
            saveDir = (String) jsonObject.get("SAVEDIR");
            saveFileName = (String) jsonObject.get("SAVEFILENAME");

            k =  ((Long) jsonObject.get("K")).intValue();
            suppression = (double) jsonObject.get("SUPPRESSION");

            JSONObject level_1 = (JSONObject) jsonObject.get("TCLOSENESS");
            JSONObject level_2 = (JSONObject) level_1.get("ORDEREDDISTANCE");

            jsonArray = (JSONArray)level_2.get("ATT");
            orderedDistanceTClosenessAtt = new String[jsonArray.size()];
            for(int i=0;i<jsonArray.size();i++){
                orderedDistanceTClosenessAtt[i]= (String) jsonArray.get(i);
            }
            jsonArray = (JSONArray)level_2.get("T");
            orderedDistanceTClosenessValue = new double[jsonArray.size()];
            for(int i=0;i<jsonArray.size();i++){
                orderedDistanceTClosenessValue[i]= (double) jsonArray.get(i);
            }

            level_2 = (JSONObject) level_1.get("EQUALDISTANCE");

            jsonArray = (JSONArray)level_2.get("ATT");
            equalDistanceTClosenessAtt = new String[jsonArray.size()];
            for(int i=0;i<jsonArray.size();i++){
                equalDistanceTClosenessAtt[i]= (String) jsonArray.get(i);
            }
            jsonArray = (JSONArray)level_2.get("T");
            equalDistanceTClosenessValue = new double[jsonArray.size()];
            for(int i=0;i<jsonArray.size();i++){
                equalDistanceTClosenessValue[i]= (double) jsonArray.get(i);
            }

            level_1 = (JSONObject) jsonObject.get("LDIVERSITY");
            level_2 = (JSONObject) level_1.get("ENTROPY");

            jsonArray = (JSONArray)level_2.get("ATT");
            entropyLDiversityAtt = new String[jsonArray.size()];
            for(int i=0;i<jsonArray.size();i++){
                entropyLDiversityAtt[i]= (String) jsonArray.get(i);
            }
            jsonArray = (JSONArray)level_2.get("L");
            entropyLDiversityValue = new double[jsonArray.size()];
            for(int i=0;i<jsonArray.size();i++){
                entropyLDiversityValue[i]= (double) jsonArray.get(i);
            }

            level_2 = (JSONObject) level_1.get("DISTINCT");

            jsonArray = (JSONArray)level_2.get("ATT");
            distinctLDiversityAtt = new String[jsonArray.size()];
            for(int i=0;i<jsonArray.size();i++){
                distinctLDiversityAtt[i]= (String) jsonArray.get(i);
            }
            jsonArray = (JSONArray)level_2.get("L");
            distinctLDiversityValue = new int[jsonArray.size()];
            for(int i=0;i<jsonArray.size();i++){
                distinctLDiversityValue[i]= ((Long) jsonArray.get(i)).intValue();
            }

            level_1 = (JSONObject) jsonObject.get("CLDIVERSITY");
            level_2 = (JSONObject) level_1.get("RECURSIVE");

            jsonArray = (JSONArray)level_2.get("ATT");
            recursiveCLDiversityAtt = new String[jsonArray.size()];
            for(int i=0;i<jsonArray.size();i++){
                recursiveCLDiversityAtt[i]= (String) jsonArray.get(i);
            }
            jsonArray = (JSONArray)level_2.get("C");
            recursiveCLDiversityCValue = new double[jsonArray.size()];
            for(int i=0;i<jsonArray.size();i++){
                recursiveCLDiversityCValue[i]= (double) jsonArray.get(i);
            }
            jsonArray = (JSONArray)level_2.get("L");
            recursiveCLDiversityLValue = new int[jsonArray.size()];
            for(int i=0;i<jsonArray.size();i++){
                recursiveCLDiversityLValue[i]= ((Long) jsonArray.get(i)).intValue();
            }

        } catch (ParseException e) {
            e.printStackTrace();
            System.err.println("Problem in json file, please check if it is correctly formatted!");
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Cannot load the file, please check if it is in the correct directory!");
        }
    }
/*
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
*/
}
