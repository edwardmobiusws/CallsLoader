package mobiusws.callsLoader;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class IncomingRecorder implements Runnable {

	private  ConcurrentHashMap<String, Integer> feedingRecorder;
	private  String fileName ;
	private  LinkedBlockingQueue<String> readyProcessHourQ ;
	public static String sperator = "_";
	
	
	public IncomingRecorder(ConcurrentHashMap<String, Integer> feedingRecorder,String fileName,  LinkedBlockingQueue<String> readyProcessHourQ) {
		 this.feedingRecorder = feedingRecorder;
		 this.fileName = fileName;
		 this.readyProcessHourQ = readyProcessHourQ;
	}
	public void run() {
    	String fileDate = fileName.substring(0, 10);
    	
    	String number  =fileName.substring(fileName.lastIndexOf('_') +1, fileName.lastIndexOf('.'));
    	Integer lastestCountNumber = Integer.parseInt(number);
    	
    	if(feedingRecorder.containsKey(fileDate)){
    		
    		if(feedingRecorder.get(fileDate) < lastestCountNumber) feedingRecorder.put(fileDate, lastestCountNumber);
    		
    	}else{// new hour is coming.
    		
    		feedingRecorder.putIfAbsent(fileDate, lastestCountNumber);
   		
    		
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
			Date now = null;
			try {
				now = sdf.parse(fileDate);
			} catch (ParseException e) {
				ExceptionToolkit.exceptionToLogWithMsg(e, " parse error, date is:"+ fileDate);
				return;
			}
			if(now != null){
				Calendar cal = Calendar.getInstance();
				cal.setTime(now);
				cal.add(Calendar.HOUR_OF_DAY, -1);
				Date lastHour = cal.getTime();
				String readyGoH = sdf.format(lastHour);	
				
				if(feedingRecorder.containsKey(readyGoH)){
					Integer nb = feedingRecorder.get(readyGoH);
					// other thread may added it first and removed it.
					if(nb!=null)readyProcessHourQ.offer(readyGoH + sperator +nb );
					feedingRecorder.remove(readyGoH);
					
				}
				
			}
			
    	}
    	
    	
	}

}
