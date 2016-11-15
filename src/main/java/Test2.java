import java.text.SimpleDateFormat;
import java.util.HashMap;

public class Test2 {

	private static SimpleDateFormat sdfYMD = null;
	private static SimpleDateFormat sdfHM = null;

	public static void main(String[] args) {

		sdfYMD = new SimpleDateFormat("yyyy-MM-dd");
		sdfHM = new SimpleDateFormat("HH-mm");
		HashMap<String, String> headers = new HashMap<String, String>();
		headers.put("timestamp", Long.toString(System.currentTimeMillis()));
		String filePath = "hdfs://dm056.tj.momo.com:8020/data/flume/%Y-%m-%d";
		String fileName = "%H-%M";
		long start = System.currentTimeMillis();
		for (int i = 0; i < 2400000; i++) {
			// String realPath = BucketPath.escapeString(filePath, headers,
			// null, false, Calendar.SECOND, 1, false);
			// String realName = BucketPath.escapeString(fileName, headers,
			// null, false, Calendar.SECOND, 1, false);

			String realPath = getTime("yyyy-MM-dd", Long.parseLong(headers.get("timestamp")));
			String realName = getTime("HH-mm", Long.parseLong(headers.get("timestamp")));
		}
		long end = System.currentTimeMillis();
		System.out.println("end time is:" + end + ".\nTotal time is:" + (end - start) + " ms.");
	}

	public static String getTime(String format, long timestamp) {
		String time = "";
		if (format.equals("HH-mm"))
			time = sdfHM.format(timestamp);
		else if (format.equals("yyyy-MM-dd"))
			time = sdfYMD.format(timestamp);
		return time;
	}
}