package LandingTest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

public class HiveConnection {
	static {
		try {
			Class.forName("org.apache.hive.jdbc.HiveDriver");
		} catch (ClassNotFoundException exception) {
			System.out.println(exception.getMessage());
		}
	}

	public static Connection getConnection(
			String hiveURL, String hiveUserName, String hivePassword) throws SQLException {
		return DriverManager.getConnection(hiveURL, hiveUserName, hivePassword);
	}

	public static void kerberosLogin(String krb5Conf, String user, String keytabPath) {
		try {
			System.setProperty("java.security.krb5.conf", krb5Conf);
            Configuration conf = new Configuration();
            conf.set("hadoop.security.authentication", "kerberos");

            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab(user, keytabPath);
        } catch (Exception e) {
            e.printStackTrace();
        }
	}
	
}
