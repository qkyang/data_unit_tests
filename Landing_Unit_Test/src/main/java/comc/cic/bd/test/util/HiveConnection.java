package comc.cic.bd.test.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

public class HiveConnection {
	static {
		try {
			Class.forName("org.apache.hive.jdbc.HiveDriver");
		} catch (ClassNotFoundException cnfe) {
			System.out.println(cnfe.getMessage());
		}
	}

	public static Connection getConnection(
			String url, String username, String password) throws SQLException {
		return DriverManager.getConnection(url, username, password);
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
