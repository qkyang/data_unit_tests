package comc.cic.bd.test.util;

public class TestTrimLast {
	public static void main(String[] args) {
		String a = "123";
		
		String b = a.substring(0, a.length() - 1);
		System.out.println(b);
	}
}
