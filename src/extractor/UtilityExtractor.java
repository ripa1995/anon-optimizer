package extractor;

import org.apache.hadoop.hdfs.web.JsonUtil;
import org.hsqldb.jdbc.JDBCStatement;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

public class UtilityExtractor {

    String db_url, user, password;
    ArrayList<String> queriesExplain;
    ArrayList<String> filterColumn;
    ArrayList<String> selectColumn;
    ArrayList<String> havingColumn;
    ArrayList<String> groupColumn;
    HashMap<String, HashMap<String,ArrayList<String>>> hashMap;

    public UtilityExtractor(){
        this.db_url="jdbc:hsqldb:file:data\\testdb";
        this.user="SA";
        this.password="";
        this.queriesExplain = new ArrayList<String>();
        this.filterColumn = new ArrayList<String>();
        this.selectColumn = new ArrayList<String>();
        this.havingColumn = new ArrayList<String>();
        this.groupColumn = new ArrayList<String>();
        this.hashMap = new HashMap<String, HashMap<String,ArrayList<String>>>();
    }

    public UtilityExtractor(String db_url, String user, String password){
        this.db_url=db_url;
        this.user=user;
        this.password=password;
        this.queriesExplain = new ArrayList<String>();
        this.filterColumn = new ArrayList<String>();
        this.selectColumn = new ArrayList<String>();
        this.havingColumn = new ArrayList<String>();
        this.groupColumn = new ArrayList<String>();
        this.hashMap = new HashMap<String, HashMap<String,ArrayList<String>>>();
    }

    public int runQueries(String[] queries){
        this.queriesExplain.clear();
        if(queries.length==0){
            return 0;
        }
        try {
            Class.forName("org.hsqldb.jdbc.JDBCDriver" );
        } catch (Exception e) {
            System.err.println("ERROR: failed to load HSQLDB JDBC driver.");
            e.printStackTrace();
            return -1;
        }

        try {
            //choose database to use
            Connection c = DriverManager.getConnection(db_url, user, password);
            JDBCStatement statement = (JDBCStatement) c.createStatement();
            String initQuery = "EXPLAIN JSON MINIMAL FOR ";
            for (String query : queries) {
                ResultSet resultSet = statement.executeQuery(initQuery + query);
                if (resultSet.next()) {
                    queriesExplain.add(resultSet.getString(1));
                }
            }
            c.close();
        } catch (SQLException e) {
            e.printStackTrace();
            this.queriesExplain.clear();
            return -1;
        }
        return queriesExplain.size();
    }

    public void extractColumn(){
        this.filterColumn.clear();
        this.selectColumn.clear();
        for(String explain:queriesExplain){
            JSONParser jsonParser = new JSONParser();
            try {
                ArrayList<String> column = new ArrayList<String>();
                Object object = jsonParser.parse(explain);
                JSONObject jsonObject = (JSONObject) object;
                jsonObject = (JSONObject) jsonObject.get("SELECT");
                jsonObject = (JSONObject) jsonObject.get("QUERYSPECIFICATION");
                JSONArray jsonArray = (JSONArray) jsonObject.get("RANGEVARIABLES");
                for (Object obj : jsonArray) {
                    column.clear();
                    findKeyExpressionColumn(obj, column);
                    for(String s:column){
                        if(!filterColumn.contains(s)){
                            filterColumn.add(s);
                        }
                    }
                }
                column.clear();
                findKeyExpressionColumn(jsonObject.get("QUERYCONDITION"), column);
                for(String s:column){
                    if(!filterColumn.contains(s)){
                        filterColumn.add(s);
                    }
                }
                column.clear();
                findKeyExpressionColumn(jsonObject.get("COLUMNS"), column);
                for(String s:column){
                    if(!selectColumn.contains(s)){
                        selectColumn.add(s);
                    }
                }
                column.clear();
                if(jsonObject.containsKey("GROUPCOLUMNS")) {
                    findKeyExpressionColumn(jsonObject.get("GROUPCOLUMNS"), column);
                    for (String s : column) {
                        if (!groupColumn.contains(s)) {
                            groupColumn.add(s);
                        }
                    }
                    if(jsonObject.containsKey("HAVINGCONDITION")){
                        column.clear();
                        findKeyExpressionColumn(jsonObject.get("HAVINGCONDITION"), column);
                        for(String s:column){
                            if(!havingColumn.contains(s)){
                                havingColumn.add(s);
                            }
                        }
                    }
                }

            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }

    private void findKeyExpressionColumn(Object object, ArrayList<String> columns) {
        if (object instanceof JSONObject) {
            JSONObject jsonObject  = (JSONObject) object;
            /*if(jsonObject.containsKey("EXPRESSION_AGGREGATE")) {
                jsonObject = (JSONObject) jsonObject.get("EXPRESSION_AGGREGATE");
                Object optype = jsonObject.get("OPTYPE");
                jsonObject = (JSONObject) jsonObject.get("ARG");
                jsonObject = (JSONObject) jsonObject.get("EXPRESSION_COLUMN");
                Object value = jsonObject.get("VALUE");
                columns.add("AGGTYPE."+optype.toString()+" "+value.toString());
            } else*/
            if(jsonObject.containsKey("EXPRESSION_LOGICAL")) {
                jsonObject = (JSONObject) jsonObject.get("EXPRESSION_LOGICAL");
                Object optype = jsonObject.get("OPTYPE");
                if (!(optype.toString().equals("AND")||optype.toString().equals("OR"))) {
                    if (jsonObject.containsKey("ARG_LEFT") && jsonObject.containsKey("ARG_RIGHT")) {
                        String arg_left = findLogicalExp(jsonObject.get("ARG_LEFT"));
                        String arg_right = findLogicalExp(jsonObject.get("ARG_RIGHT"));
                        columns.add("LOGIC." + optype.toString() + " " + arg_left.trim() + " " + arg_right.trim());
                    } else if (jsonObject.containsKey("ARG_LEFT")) {
                        String arg_left = findLogicalExp(jsonObject.get("ARG_LEFT"));
                        columns.add("LOGIC." + optype.toString() + " " + arg_left.trim());
                    }
                } else {
                    for (Object key : jsonObject.keySet()) {
                        Object internalJson = (Object) jsonObject.get(key.toString());
                        findKeyExpressionColumn(internalJson,columns);
                    }
                }
            } else if (jsonObject.containsKey("EXPRESSION_COLUMN")) {
                jsonObject = (JSONObject) jsonObject.get("EXPRESSION_COLUMN");
                Object value = jsonObject.get("VALUE");
                columns.add(value.toString());
            } else {
                for (Object key : jsonObject.keySet()) {
                    Object internalJson = (Object) jsonObject.get(key.toString());
                    findKeyExpressionColumn(internalJson,columns);
                }
            }
        } else if (object instanceof JSONArray) {
            for (Object internalObj : (JSONArray) object) {
                findKeyExpressionColumn(internalObj, columns);
            }
        }
    }

    private String findLogicalExp(Object arg) {
        if (arg instanceof JSONObject) {
            JSONObject jsonObject  = (JSONObject) arg;
            if (jsonObject.containsKey("EXPRESSION_COLUMN")) {
                jsonObject = (JSONObject) jsonObject.get("EXPRESSION_COLUMN");
                Object value = jsonObject.get("VALUE");
                return value.toString();
            } else if (jsonObject.containsKey("EXPRESSION_VALUE")) {
                jsonObject = (JSONObject) jsonObject.get("EXPRESSION_VALUE");
                Object value = jsonObject.get("VALUE");
                return value.toString();
            } else {
                String s = "";
                for (Object key : jsonObject.keySet()) {
                    Object internalJson = (Object) jsonObject.get(key.toString());
                    s = s + findLogicalExp(internalJson);
                }
                return s;
            }
        } else if (arg instanceof JSONArray) {
            String s = "";
            for (Object internalObj : (JSONArray) arg) {
                s = s + findLogicalExp(internalObj) + " ";
            }
            return s;
        }
        return "";
    }

    public void generateWorkloadUtility(){
        hashMap = new HashMap<String, HashMap<String,ArrayList<String>>>();
        ArrayList<String> allColumn = new ArrayList<>();
        allColumn.addAll(filterColumn);
        allColumn.addAll(selectColumn);
        allColumn.addAll(havingColumn);
        allColumn.addAll(groupColumn);
        for(String s:allColumn){
            s = s.trim();
            String[] splitted = s.split(" ");
            if(splitted.length==1){
                String[] fullColumnName = splitted[0].split("[.]");
                hashMap.putIfAbsent(fullColumnName[fullColumnName.length-1],new HashMap<String,ArrayList<String>>());
            } else {
                String[] fullColumnName = splitted[1].split("[.]");
                String columnName = fullColumnName[fullColumnName.length-1];
                HashMap<String,ArrayList<String>> conditionMap = new HashMap<String,ArrayList<String>>();
                ArrayList<String> values = new ArrayList<>();
                for(int i=2; i<splitted.length;i++){
                    if(!splitted[i].equals("")) {
                        values.add(splitted[i].replace("'",""));
                    }
                }
                if (hashMap.containsKey(columnName)){
                    conditionMap = hashMap.get(columnName);
                    if(conditionMap.containsKey(splitted[0])) {
                        values.addAll(conditionMap.get(splitted[0]));
                    }
                }
                conditionMap.put(splitted[0],values);
                hashMap.put(columnName,conditionMap);
            }
        }
    }

    public HashMap<String, HashMap<String, ArrayList<String>>> getWorkloadUtility() {
        return hashMap;
    }

    public ArrayList<String> getQueriesExplain() {
        return queriesExplain;
    }

    public ArrayList<String> getFilterColumn() {
        return filterColumn;
    }

    public ArrayList<String> getSelectColumn() {
        return selectColumn;
    }

    public ArrayList<String> getHavingColumn() {
        return havingColumn;
    }

    public ArrayList<String> getGroupColumn() {
        return groupColumn;
    }

    public String getWorkloadUtilityResult(){
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("WORKLOAD: ");
        stringBuilder.append("\n");
        for(String key:hashMap.keySet()){
            stringBuilder.append("-COLUMN: ").append(key);
            stringBuilder.append("\n");
            for(String type:hashMap.get(key).keySet()){
                stringBuilder.append("--TYPE: ").append(type);
                stringBuilder.append("\n");
                for(String value:hashMap.get(key).get(type)){
                    stringBuilder.append("---VALUE: ").append(value);
                    stringBuilder.append("\n");
                }
            }
        }
        return stringBuilder.toString();
    }
}
