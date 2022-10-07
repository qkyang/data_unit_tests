package LandingTest;

import com.opencsv.CSVWriter;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;

public class Debug {
    public static void main(String[] args) throws Exception {
        // NOTE: This class is for debugging reason, would be deleted after development

//        Properties prop = new Properties();
//        String propFileName = "data.properties";
//        ClassLoader loader = Thread.currentThread().getContextClassLoader();
//        try (InputStream resourceStream = loader.getResourceAsStream(propFileName)) {
//            prop.load(resourceStream);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        String database = prop.getProperty("database");
//        String tableName = prop.getProperty("tableName");
//        String csvFilePath = prop.getProperty("insertTables");
//        String delimiter = prop.getProperty("delimiter");
//
//        File file = new File(csvFilePath);
//        for(String fileNames : file.list()) System.out.println(fileNames);

//        Properties prop = new Properties();
//        String propFileName = "DBConnection.properties";
//        ClassLoader loader = Thread.currentThread().getContextClassLoader();
//        try(InputStream resourceStream = loader.getResourceAsStream(propFileName)) {
//            prop.load(resourceStream);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        String driverClass = prop.getProperty("driverClass");
//        String connectionURL = prop.getProperty("connectionURL");
//        String userName = prop.getProperty("userName");
//        String password = prop.getProperty("password", "");
//        String krb5Conf = prop.getProperty("krb5Conf");
//        String authMethod = prop.getProperty("authMethod","kerberos");
//        String keytabPath = prop.getProperty("keytabPath");

//        CSVToXML xmlCreator = new CSVToXML();
//        xmlCreator.convertFile(".\\ibor.student.csv", ".\\ibor.student.xml", ",", "\'");

//        InsertCommandBuilder insertCommandBuilder=new InsertCommandBuilder();
//        insertCommandBuilder.insertCommand();

//        TruncateCommandBuilder truncateCommandBuilder = new TruncateCommandBuilder();
//        truncateCommandBuilder.truncateCommand();

//        File file = new File("./InsertTables/ibor.student.csv");
//        String fileNameWithOutExt = FilenameUtils.removeExtension(file.getName());
//        String database = fileNameWithOutExt.split("\\.")[0];
//        String table = fileNameWithOutExt.split("\\.")[1];
//        System.out.println(database);
//        System.out.println(table);

//        // NOTE: Read the properties
//        Properties prop = new Properties();
//        String propFileName = "data.properties";
//        ClassLoader loader = Thread.currentThread().getContextClassLoader();
//        try (InputStream resourceStream = loader.getResourceAsStream(propFileName)) {
//            prop.load(resourceStream);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        String SQLPath = prop.getProperty("SQLPath");
//
//        // NOTE: Actual table
//        // Get content of the table
//        File file = new File(SQLPath);
//        for (String fileName : file.list()) {
//            System.out.println("[INFO] Directory contains SQL file " + fileName);
//            String sql;
//            sql = new String(Files.readAllBytes(Paths.get(SQLPath+"/"+fileName)));
//            System.out.println("[INFO] SQL file contains " + sql);
//        }

//        String inputFile = readProperty("data.properties","inputFile");
//        System.out.println(inputFile);

        List<CSVRecord> csvRecords = new ArrayList<>();
        CSVFormat csvFormat = CSVFormat.newFormat(',');
        csvFormat = csvFormat.withSkipHeaderRecord(false);
        csvFormat = csvFormat.withIgnoreEmptyLines(true);
        csvFormat = csvFormat.withQuote('"');

        try (FileInputStream fis = new FileInputStream("./TestDic_HUIJIN.csv");
             InputStreamReader isr = new InputStreamReader(fis, "utf-8");
             CSVParser csvParser = new CSVParser(isr, csvFormat)) {
            Consumer<CSVRecord> consumer = record -> {
                csvRecords.add(record);
            };
            csvParser.forEach(consumer);
        }

        Path file = Paths.get("./hiveOutputCSV/table-ordering.txt");
        Files.write(file,"".getBytes());
        // NOTE: Comparison
        for (CSVRecord csvRecord : csvRecords) {
            String hiveTableName = csvRecord.get(1);
            String oracleTableName = csvRecord.get(0);
            System.out.println("[INFO] This record is " + oracleTableName + ", " + hiveTableName);


            Files.write(file, hiveTableName.getBytes(), StandardOpenOption.APPEND);
            Files.write(file, "\r\n".getBytes(), StandardOpenOption.APPEND);
        }
    }

    private static String readProperty(String propertyFileName, String propertyKey) {
        String property;
        Properties prop = new Properties();
        String propFileName = propertyFileName;
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        try (InputStream resourceStream = loader.getResourceAsStream(propFileName)) {
            prop.load(resourceStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        property = prop.getProperty(propertyKey);
        return property;
    }
}
