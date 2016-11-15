import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TestDate {

	public static void main(String[] args) throws Exception {
		 SimpleDateFormat  formatter = new SimpleDateFormat ("yyyy-MM-dd HH:mm:ss") ;
		
		
		String time = "2016-10-13 14:47:41" ;
		
		System.out.println(time +"    "+ new Date(formatter.parse(time).getTime()));

		
		SimpleDateFormat  formatter2 = new SimpleDateFormat ("YYYY-MM-ddHH:mm:ss.SSS") ;
		
		System.out.println(time +"    "+ new Date(formatter2.parse("2016-10-2509:25:50.967").getTime()));

	}

}
