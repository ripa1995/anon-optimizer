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



    public static void main(String[] args) {
        Initializer.start();
    }

}
