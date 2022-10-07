package LandingTest;

import com.opencsv.CSVWriter;
import junit.framework.TestCase;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.dbunit.Assertion;
import org.dbunit.IDatabaseTester;
import org.dbunit.JdbcDatabaseTester;
import org.dbunit.dataset.*;
import org.dbunit.dataset.csv.CsvDataSet;
import org.dbunit.dataset.filter.DefaultColumnFilter;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.function.Consumer;

public class TestCases extends TestCase {

    private IDatabaseTester oracleDBTester;
    private IDatabaseTester hiveDBTester;

    private String hiveOutputCSVPath;
    private String oracleSQLPath;
    //    private String tableList;
    private String testDictionary;
    //    private String allColumnList;
    private Connection connection;
    private Statement statement;
    private ResultSet result;

    private static final int HIVE_DB_INDEX = 0;
    private static final int TABLE_NAME_INDEX = 1;
    //    private static final int ORACLE_TABLE_INDEX = 0;
    //    private static final int HIVE_TABLE_INDEX = 1;
    private static final int COLUMN_NAME_INDEX = 2;
    private static final int COLUMN_DATA_TYPE_INDEX = 3;

    public TestCases(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        // NOTE: READ THE PROPERTIES
        String oracleDriver = readProperty("DBConnection.properties", "oracleDriver");
        String hiveDriver = readProperty("DBConnection.properties", "hiveDriver");
        String oracleURL = readProperty("DBConnection.properties", "oracleURL");
        String hiveURL = readProperty("DBConnection.properties", "hiveURL");
        String oracleUserName = readProperty("DBConnection.properties", "oracleUserName");
        String hiveUserName = readProperty("DBConnection.properties", "hiveUserName");
        String oraclePassword = readProperty("DBConnection.properties", "oraclePassword");
        String hivePassword = readProperty("DBConnection.properties", "hivePassword");

        String krb5Conf = readProperty("DBConnection.properties", "krb5Conf");
        String authMethod = readProperty("DBConnection.properties", "authMethod");
        String keytabPath = readProperty("DBConnection.properties", "keytabPath");

        // NOTE: DB CONNECTION
        try {
            System.setProperty("java.security.krb5.conf", krb5Conf);
            Configuration conf = new Configuration();
            conf.set("hadoop.security.authentication", authMethod);
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab(hiveUserName, keytabPath);
        } catch (Exception e) {
            e.printStackTrace();
        }

        oracleDBTester = new JdbcDatabaseTester(oracleDriver, oracleURL, oracleUserName, oraclePassword);
        hiveDBTester = new JdbcDatabaseTester(hiveDriver, hiveURL, hiveUserName, hivePassword);

        // NOTE: Setup input and output
        hiveOutputCSVPath = readProperty("data.properties", "hiveOutputCSVPath");
        oracleSQLPath = readProperty("data.properties", "oracleSQLPath");
//        tableList = readProperty("data.properties", "tableList");
        testDictionary = readProperty("data.properties", "testDictionary");
//        allColumnList = readProperty("data.properties", "allColumnList");

        // NOTE: Set execution engine
        connection = hiveDBTester.getConnection().getConnection();
        statement = connection.createStatement();
        String setEngine = "set hive.execution.engine=spark";
        statement.execute(setEngine);
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

    private List<CSVRecord> readCSVFile(String inputFile) throws Exception {
        List<CSVRecord> csvRecords = new ArrayList<>();

        CSVFormat csvFormat = CSVFormat.newFormat(',');
        csvFormat = csvFormat.withSkipHeaderRecord(false);
        csvFormat = csvFormat.withIgnoreEmptyLines(true);
        csvFormat = csvFormat.withQuote('"');

        try (FileInputStream fis = new FileInputStream(inputFile);
             InputStreamReader isr = new InputStreamReader(fis, "utf-8");
             CSVParser csvParser = new CSVParser(isr, csvFormat)) {
            Consumer<CSVRecord> consumer = csvRecords::add;
            csvParser.forEach(consumer);
        }

        return csvRecords;
    }

    private void generateCSVTableColumnList(String path, List<CSVRecord> csvRecords) throws Exception {
        Path file = Paths.get(path);
        Files.write(file, "".getBytes());
        for (CSVRecord csvRecord : csvRecords) {
            String hiveTableName = csvRecord.get(HIVE_DB_INDEX) + "." + csvRecord.get(TABLE_NAME_INDEX);
            String hiveColumnName = csvRecord.get(COLUMN_NAME_INDEX);
            String tableColumnName = hiveTableName + "." + hiveColumnName;
            Files.write(file, tableColumnName.getBytes(), StandardOpenOption.APPEND);
            Files.write(file, "\r\n".getBytes(), StandardOpenOption.APPEND);
        }
    }

    private void generateCSVTableList(String path, List<CSVRecord> csvRecords) throws Exception {
        Path file = Paths.get(path);
        Files.write(file, "".getBytes());
        List<String> tableNames = new ArrayList<>();
        for (CSVRecord csvRecord : csvRecords) {
            String hiveTableName = csvRecord.get(HIVE_DB_INDEX) + "." + csvRecord.get(TABLE_NAME_INDEX);
            tableNames.add(hiveTableName);
        }
        Set<String> uniqueTableNames = new HashSet<>(tableNames);
        for (String tableName : uniqueTableNames) {
            Files.write(file, tableName.getBytes(), StandardOpenOption.APPEND);
            Files.write(file, "\r\n".getBytes(), StandardOpenOption.APPEND);
        }
    }

    private void generateCSVFile(String outputPath, String tableName, ResultSet resultSet, boolean isColumnNameModified) throws Exception {
        List<String[]> columnNameRow = null;
        // NOTE: Get the modified column names
        if (isColumnNameModified)
            columnNameRow = changeColumnNames(resultSet);
        // NOTE: Write the content to a CSV file
        String hiveTableCSVPathName = outputPath + "/" + tableName + ".csv";
        System.out.println("Generating CSV file " + hiveTableCSVPathName);
        Path csvFile = Paths.get(hiveTableCSVPathName);
        Files.write(csvFile, "".getBytes());
        FileWriter fileWriter = new FileWriter(hiveTableCSVPathName, true);
        CSVWriter csvWriter = new CSVWriter(fileWriter);
        if (isColumnNameModified) {
            csvWriter.writeAll(columnNameRow);
            csvWriter.writeAll(resultSet, false);
        } else {
            csvWriter.writeAll(resultSet, true);
        }
        csvWriter.close();
    }

    private List<String> getColumnNames(ResultSet resultSet) throws SQLException {
        ArrayList<String> newColumnNamesArrayList = new ArrayList<>();
        for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
            String columnName = resultSet.getMetaData().getColumnName(i);
            String newColumnName = columnName.split("\\.")[1].toUpperCase();
            System.out.println("The modified column name is " + newColumnName);
            newColumnNamesArrayList.add(newColumnName);
        }
        return newColumnNamesArrayList;
    }

    private List<String[]> changeColumnNames(ResultSet resultSet) throws Exception {
        ArrayList<String> newColumnNamesArrayList = new ArrayList<>();
        for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
            String columnName = resultSet.getMetaData().getColumnName(i);
            String newColumnName = columnName.split("\\.")[1].toUpperCase();
            System.out.println("The modified column name is " + newColumnName);
            newColumnNamesArrayList.add(newColumnName);
        }
        String[] newColumnNames = newColumnNamesArrayList.toArray(new String[0]);
        List<String[]> columnNameRow = new ArrayList<>();
        columnNameRow.add(newColumnNames);
        return columnNameRow;
    }

    private void replaceEscapeCharsInCSV(String CSVPath) throws IOException {
        File file = new File(CSVPath);
        for (String fileName : file.list()) {
            if (fileName.contains(".txt")) continue;
            Path path = Paths.get(CSVPath + "/" + fileName);
            Charset charset = StandardCharsets.UTF_8;

            String content = new String(Files.readAllBytes(path), charset);
            content = content.replaceAll("\\\\", "\\\\\\\\");
            Files.write(path, content.getBytes(charset));
        }
    }

    @Test
    public void test_Compare_Entire_Table() throws Exception {
        // NOTE: Read input file
//        List<CSVRecord> tableListCSVRecords = readCSVFile(tableList);
        List<CSVRecord> tableColumnListCSVRecords = readCSVFile(testDictionary);

        // NOTE: Generate CSV list
        String CSVTableListPath = hiveOutputCSVPath + "/" + "table-ordering.txt";
        generateCSVTableColumnList(CSVTableListPath, tableColumnListCSVRecords);

        // NOTE: Generate CSV files
        for (CSVRecord csvRecord : tableColumnListCSVRecords) {
            String hiveTableName = csvRecord.get(HIVE_DB_INDEX) + "." + csvRecord.get(TABLE_NAME_INDEX);
            String hiveColumnName = csvRecord.get(COLUMN_NAME_INDEX);
            if (csvRecord.get(COLUMN_DATA_TYPE_INDEX).equals("date")) continue;
            if (csvRecord.get(COLUMN_DATA_TYPE_INDEX).equals("timestamp")) continue;
            result = statement.executeQuery("select " + hiveColumnName + " from " + hiveTableName + " order by cast(" + hiveColumnName + " as string)");
            generateCSVFile(hiveOutputCSVPath, hiveTableName + "." + hiveColumnName, result, false);
        }
        replaceEscapeCharsInCSV(hiveOutputCSVPath);

        // NOTE: Comparison
        String failureMsg = "";
        File file = new File(oracleSQLPath);
        for (CSVRecord csvRecord : tableColumnListCSVRecords) {
            String hiveTableName = csvRecord.get(HIVE_DB_INDEX) + "." + csvRecord.get(TABLE_NAME_INDEX);
            String oracleTableName = csvRecord.get(TABLE_NAME_INDEX);
            String columnName = csvRecord.get(COLUMN_NAME_INDEX);
            if (csvRecord.get(COLUMN_DATA_TYPE_INDEX).equals("date")) continue;
            if (csvRecord.get(COLUMN_DATA_TYPE_INDEX).equals("timestamp")) continue;
            System.out.println("[INFO] Table names are " + oracleTableName + ", " + hiveTableName + " And the column name is " + columnName);

            // NOTE: Oracle table
            String oracleSQL = "";
            for (String fileName : file.list()) {
                if (fileName.equals(oracleTableName + ".sql"))
                    oracleSQL = new String(Files.readAllBytes(Paths.get(oracleSQLPath + "/" + fileName)));
            }
            oracleSQL = "select " + columnName + " from (" + oracleSQL + ") order by NLSSORT(" + columnName + ",'NLS_SORT=unicode_binary') nulls first";
            ITable oracleTable = oracleDBTester.getConnection().createQueryTable(oracleTableName, oracleSQL);

            // NOTE: Hive table
            IDataSet hiveDataSet = new CsvDataSet(new File(hiveOutputCSVPath));
            // Handle null strings
            ReplacementDataSet replacedHiveDataSet = new ReplacementDataSet(hiveDataSet);
            replacedHiveDataSet.addReplacementObject("", null);
            ITable hiveTable = replacedHiveDataSet.getTable(hiveTableName + "." + columnName);
//            ITable hiveTable = hiveDataSet.getTable(hiveTableName);

            // NOTE: Filter the processing time column
//            DefaultColumnFilter columnFilter = new DefaultColumnFilter();
//            columnFilter.excludeColumn("*processing_dttm");
//            FilteredTableMetaData filteredTableMetaData = new FilteredTableMetaData(hiveTable.getTableMetaData(), columnFilter);
//            ITable filteredHiveTable = new CompositeTable(filteredTableMetaData, hiveTable);

            try {
                Assertion.assertEquals(oracleTable, hiveTable);
            } catch (AssertionError assertionError) {
                failureMsg = failureMsg + assertionError.getMessage() + "\r\n";
            }
        }
        if (!failureMsg.isEmpty()) {
            Assert.fail(failureMsg);
        }
    }

    @Test
    public void test_Compare_Columns_Distinct_Count() throws Exception {
        // NOTE: Read input file
//        List<CSVRecord> tableListCSVRecords = readCSVFile(tableList);
        List<CSVRecord> tableColumnListCSVRecords = readCSVFile(testDictionary);

        // NOTE: Generate CSV list
        String CSVTableListPath = hiveOutputCSVPath + "/" + "table-ordering.txt";
        generateCSVTableColumnList(CSVTableListPath, tableColumnListCSVRecords);

        // NOTE: Generate CSV files
        for (CSVRecord csvRecord : tableColumnListCSVRecords) {
            String hiveTableName = csvRecord.get(HIVE_DB_INDEX) + "." + csvRecord.get(TABLE_NAME_INDEX);
            String hiveColumnName = csvRecord.get(COLUMN_NAME_INDEX);
            result = statement.executeQuery("select count(distinct " + hiveColumnName + ") as count from " + hiveTableName);
            generateCSVFile(hiveOutputCSVPath, hiveTableName + "." + hiveColumnName, result, false);
        }
//        replaceEscapeCharsInCSV(hiveOutputCSVPath);

        // NOTE: Comparison
        String failureMsg = "";
        File file = new File(oracleSQLPath);
        for (CSVRecord csvRecord : tableColumnListCSVRecords) {
            String hiveTableName = csvRecord.get(HIVE_DB_INDEX) + "." + csvRecord.get(TABLE_NAME_INDEX);
            String oracleTableName = csvRecord.get(TABLE_NAME_INDEX);
            String columnName = csvRecord.get(COLUMN_NAME_INDEX);
            System.out.println("[INFO] Table names are " + oracleTableName + ", " + hiveTableName + " And the column name is " + columnName);

            // NOTE: Oracle table
            String oracleSQL = "";
            for (String fileName : file.list()) {
                if (fileName.equals(oracleTableName + ".sql"))
                    oracleSQL = new String(Files.readAllBytes(Paths.get(oracleSQLPath + "/" + fileName)));
            }
            oracleSQL = "select count(distinct " + columnName + ") as count from (" + oracleSQL + ")";
            ITable oracleTable = oracleDBTester.getConnection().createQueryTable(oracleTableName, oracleSQL);

            // NOTE: Hive table
            IDataSet hiveDataSet = new CsvDataSet(new File(hiveOutputCSVPath));
            // Handle null strings
            ReplacementDataSet replacedHiveDataSet = new ReplacementDataSet(hiveDataSet);
            replacedHiveDataSet.addReplacementObject("", null);
            ITable hiveTable = replacedHiveDataSet.getTable(hiveTableName + "." + columnName);
//            ITable hiveTable = hiveDataSet.getTable(hiveTableName);

            // NOTE: Filter the processing time column
//            DefaultColumnFilter columnFilter = new DefaultColumnFilter();
//            columnFilter.excludeColumn("*processing_dttm");
//            FilteredTableMetaData filteredTableMetaData = new FilteredTableMetaData(hiveTable.getTableMetaData(), columnFilter);
//            ITable filteredHiveTable = new CompositeTable(filteredTableMetaData, hiveTable);

            try {
                Assertion.assertEquals(oracleTable, hiveTable);
            } catch (AssertionError assertionError) {
                failureMsg = failureMsg + assertionError.getMessage() + " " + columnName + "\r\n";
            }
        }
        if (!failureMsg.isEmpty()) {
            Assert.fail(failureMsg);
        }
    }

    @Test
    public void test_Compare_Row_Numbers() throws Exception {
        // NOTE: Read input file
        List<CSVRecord> csvRecords = readCSVFile(testDictionary);

        // NOTE: Generate CSV list
        String CSVTableListPath = hiveOutputCSVPath + "/" + "table-ordering.txt";
        generateCSVTableList(CSVTableListPath, csvRecords);

        // NOTE: Get unique values for tables
        List<String> tableNames = new ArrayList<>();
        for (CSVRecord csvRecord : csvRecords) {
            String hiveTableName = csvRecord.get(HIVE_DB_INDEX) + "." + csvRecord.get(TABLE_NAME_INDEX);
            tableNames.add(hiveTableName);
        }
        Set<String> uniqueTableNames = new HashSet<>(tableNames);

        // NOTE: Execute Hive SQL
        for (String hiveTableName : uniqueTableNames) {
//            String hiveTableName = csvRecord.get(HIVE_TABLE_INDEX);
            result = statement.executeQuery("select count(1) as count from " + hiveTableName);
            generateCSVFile(hiveOutputCSVPath, hiveTableName, result, false);
        }

        // NOTE: Comparison
        String failureMsg = "";
        File file = new File(oracleSQLPath);
        for (String hiveTableName : uniqueTableNames) {
//            String hiveTableName = csvRecord.get(HIVE_TABLE_INDEX);
//            String oracleTableName = csvRecord.get(ORACLE_TABLE_INDEX);
            String oracleTableName = hiveTableName.split("\\.")[1];
            System.out.println("[INFO] This record is " + oracleTableName + ", " + hiveTableName);

            // NOTE: Oracle table
            String oracleSQL = "";
            for (String fileName : file.list()) {
                if (fileName.equals(oracleTableName + ".sql"))
                    oracleSQL = new String(Files.readAllBytes(Paths.get(oracleSQLPath + "/" + fileName)));
            }
            ITable oracleTable = oracleDBTester.getConnection().createQueryTable(oracleTableName, "select count(1) as count from (" + oracleSQL + ")");

            // NOTE: Hive table
            IDataSet hiveDataSet = new CsvDataSet(new File(hiveOutputCSVPath));
            ITable hiveTable = hiveDataSet.getTable(hiveTableName);

            // NOTE: Filter the processing time column
            DefaultColumnFilter columnFilter = new DefaultColumnFilter();
            columnFilter.excludeColumn("*processing_dttm");
            FilteredTableMetaData filteredTableMetaData = new FilteredTableMetaData(hiveTable.getTableMetaData(), columnFilter);
            ITable filteredHiveTable = new CompositeTable(filteredTableMetaData, hiveTable);

            try {
                Assertion.assertEquals(oracleTable, filteredHiveTable);
            } catch (AssertionError assertionError) {
                failureMsg = failureMsg + assertionError.getMessage() + "\r\n";
            }
        }
        if (!failureMsg.isEmpty()) {
            Assert.fail(failureMsg);
        }
    }

    @Test
    public void test_Compare_Columns() throws Exception {
        // NOTE: Read input file
        List<CSVRecord> csvRecords = readCSVFile(testDictionary);

        // NOTE: Generate CSV list
        String CSVTableListPath = hiveOutputCSVPath + "/" + "table-ordering.txt";
        generateCSVTableList(CSVTableListPath, csvRecords);

        // NOTE: Get unique values for tables
        List<String> tableNames = new ArrayList<>();
        for (CSVRecord csvRecord : csvRecords) {
            String hiveTableName = csvRecord.get(HIVE_DB_INDEX) + "." + csvRecord.get(TABLE_NAME_INDEX);
            tableNames.add(hiveTableName);
        }
        Set<String> uniqueTableNames = new HashSet<>(tableNames);

        // NOTE: Execute Hive SQL
        for (String hiveTableName : uniqueTableNames) {
//            String hiveTableName = csvRecord.get(HIVE_TABLE_INDEX);
            result = statement.executeQuery("select * from " + hiveTableName + " limit 0");
            generateCSVFile(hiveOutputCSVPath, hiveTableName, result, false);
        }

        // NOTE: Generate CSV files
//        generateCSVFiles(COMPARE_COLUMNS, csvRecords, true);

        // NOTE: Comparison
        String failureMsg = "";
        File file = new File(oracleSQLPath);
        for (String hiveTableName : uniqueTableNames) {
//            String hiveTableName = csvRecord.get(HIVE_TABLE_INDEX);
//            String oracleTableName = csvRecord.get(ORACLE_TABLE_INDEX);
            String oracleTableName = hiveTableName.split("\\.")[1];
            System.out.println("[INFO] This record is " + oracleTableName + ", " + hiveTableName);

            // NOTE: Oracle table
            String oracleSQL = "";
            for (String fileName : file.list()) {
                if (fileName.equals(oracleTableName + ".sql"))
                    oracleSQL = new String(Files.readAllBytes(Paths.get(oracleSQLPath + "/" + fileName)));
            }
            ITable oracleTable = oracleDBTester.getConnection().createQueryTable(oracleTableName, "SELECT * FROM (" + oracleSQL + ") where ROWNUM < 1");

            // NOTE: Hive table
            IDataSet hiveDataSet = new CsvDataSet(new File(hiveOutputCSVPath));
            ReplacementDataSet replacedHiveDataSet = new ReplacementDataSet(hiveDataSet);
            replacedHiveDataSet.addReplacementObject("", null);
            ITable hiveTable = replacedHiveDataSet.getTable(hiveTableName);

            // NOTE: Filter the processing time column
            DefaultColumnFilter columnFilter = new DefaultColumnFilter();
            columnFilter.excludeColumn("*processing_dttm");
            FilteredTableMetaData filteredTableMetaData = new FilteredTableMetaData(hiveTable.getTableMetaData(), columnFilter);
            ITable filteredHiveTable = new CompositeTable(filteredTableMetaData, hiveTable);

            try {
                Assertion.assertEquals(oracleTable, filteredHiveTable);
            } catch (AssertionError assertionError) {
                failureMsg = failureMsg + assertionError.getMessage() + "\r\n";
            }
        }
        if (!failureMsg.isEmpty()) {
            Assert.fail(failureMsg);
        }
    }
}