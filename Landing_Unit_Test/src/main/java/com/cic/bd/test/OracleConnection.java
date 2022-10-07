package LandingTest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class OracleConnection {
    static {
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
        } catch (ClassNotFoundException exception) {
            System.out.println(exception.getMessage());
        }
    }

    public static Connection getConnection(
            String oracleURL, String oracleUserName, String oraclePassword) throws SQLException {
        return DriverManager.getConnection(oracleURL, oracleUserName, oraclePassword);
    }

}
