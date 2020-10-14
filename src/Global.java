import java.io.File;
import java.io.IOException;
import java.util.Calendar;

/**
 * @author tsaimx
 */
public class Global {

    public static String getWorkingDirectory() {
    	File curDir = new File(".");
    	try {
			return curDir.getCanonicalPath();
		} catch (IOException e) {
			// TODO 自動產生的 catch 區塊
			e.printStackTrace();
			
			return null;
		}
    }

	//Date-Time routine
    public static String getCurrentTime() {
    	Calendar cal = Calendar.getInstance();
    	
    	return String.format("%02d:%02d:%02d", cal.get(Calendar.HOUR_OF_DAY), cal.get(Calendar.MINUTE), cal.get(Calendar.SECOND));
    }
    
    public static String getCurrentDate() {
    	Calendar cal = Calendar.getInstance();
    	
    	return String.format("%04d/%02d/%02d", cal.get(Calendar.YEAR), cal.get(Calendar.MONTH)+1, cal.get(Calendar.DATE));
    }
    
    public static String getCurrentDateTime() {
    	return getCurrentDate() + " " + getCurrentTime();
    }
    
    public static String getCurrentTimeWithMS() {
    	Calendar cal = Calendar.getInstance();
    	
    	return String.format("%02d:%02d:%02d.%03d", cal.get(Calendar.HOUR_OF_DAY), cal.get(Calendar.MINUTE), cal.get(Calendar.SECOND), cal.get(Calendar.MILLISECOND));
    }  
    
    public static long getCurrentTimeInMillis() {
    	Calendar cal = Calendar.getInstance();
   	
    	return cal.getTimeInMillis();
    }
}

