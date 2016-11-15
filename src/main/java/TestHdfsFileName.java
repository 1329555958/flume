import java.util.Calendar;
import java.util.HashMap;

import org.apache.flume.formatter.output.BucketPath;

public class TestHdfsFileName {
	public static void main(String[] args) {
		HashMap<String, String> headers = new HashMap<String, String>();
		headers.put("timestamp", Long.toString(System.currentTimeMillis()));
		String filePath = "hdfs://xxxx.com:8020/data/flume/%Y-%m-%d";
		String fileName = "%H-%M";
		long start = System.currentTimeMillis();
		System.out.println("start time is:" + start);
		for (int i = 0; i < 2400000; i++) {
//		for (int i = 0; i < 10000; i++) {
			String realPath = BucketPath.escapeString(filePath, headers, null, false, Calendar.SECOND, 1, false);
			String realName = BucketPath.escapeString(fileName, headers, null, false, Calendar.SECOND, 1, false);
		}
		long end = System.currentTimeMillis();
		System.out.println("end time is:" + end + ".\nTotal time is:" + (end - start) + " ms.");
	}

}
