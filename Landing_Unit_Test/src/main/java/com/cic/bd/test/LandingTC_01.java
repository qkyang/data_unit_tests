package LandingTest;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class LandingTC_01 {
    private static Logger logger = Logger.getLogger(LandingTC_01.class);
//    private static Logger tcLanding06Logger = Logger.getLogger("tcLanding06");

    private static final String TEST_CASE_NAME = "LandingTC_01";
    private static final int TABLE_NAME_INDEX = 1;

    @Test
    @Parameters({"krb5Conf", "kerberosUser", "keytabPath",
            "oracleURL", "oracleUserName", "oraclePassword",
            "hiveURL", "hiveUserName", "hivePassword",
            "inputFile"})
    public void test(String krb5Conf, String kerberosUser, String keytabPath,
                     String oracleURL, String oracleUserName, String oraclePassword,
                     String hiveURL, String hiveUserName, String hivePassword,
                     String inputFile) throws Exception {

        // NOTE: READING INPUT FILE
        logger.info(TEST_CASE_NAME + " start reading input file");
        List<CSVRecord> csvRecords = new ArrayList<>();
        CSVFormat csvFormat = CSVFormat.newFormat(',');
        csvFormat = csvFormat.withSkipHeaderRecord(false);
        csvFormat = csvFormat.withIgnoreEmptyLines(true);
        csvFormat = csvFormat.withQuote('"');

        try (FileInputStream fis = new FileInputStream(inputFile);
             InputStreamReader isr = new InputStreamReader(fis, "utf-8");
             CSVParser csvParser = new CSVParser(isr, csvFormat)) {
            Consumer<CSVRecord> consumer = record -> {
                logger.debug(TEST_CASE_NAME + " handle line:" + record.getRecordNumber() + ", record:" + record.toString());
                csvRecords.add(record);
            };
            csvParser.forEach(consumer);
        }
        logger.info(TEST_CASE_NAME + " end reading input file");

        // NOTE: CONNECT TO ORACLE & HIVE
        HiveConnection.kerberosLogin(krb5Conf, kerberosUser, keytabPath);
        logger.debug(TEST_CASE_NAME + " Hive Kerberos connection succeeded");
        try (
                Connection oracleConnection = OracleConnection.getConnection(oracleURL, oracleUserName, oraclePassword);
                Statement expectedStatement = oracleConnection.createStatement();
                Connection hiveConnection = HiveConnection.getConnection(hiveURL, hiveUserName, hivePassword);
                Statement actualStatement = hiveConnection.createStatement()) {

            // NOTE: COMPARE ORACLE & HIVE
            for (CSVRecord csvRecord : csvRecords) {
                String tableName = csvRecord.get(TABLE_NAME_INDEX);
                List<String> messages = new ArrayList<>();

//                logger.debug(TEST_CASE_NAME + " start checking for table " + tableName);
                String SQLCommand = "select * from " + tableName;
                logger.debug(TEST_CASE_NAME + " " + SQLCommand);

                try (ResultSet expectedResultSet = expectedStatement.executeQuery(SQLCommand);
                     ResultSet actualResultSet = actualStatement.executeQuery(SQLCommand)) {
                    Assert.assertEquals(expectedResultSet, actualResultSet);
                } catch (Exception e) {
                    messages.add(tableName + " encountered exception " + e.getMessage());
                }
                logger.debug(TEST_CASE_NAME + " end checking for " + tableName);
            }
        }

        logger.info(TEST_CASE_NAME + " end");
    }
}