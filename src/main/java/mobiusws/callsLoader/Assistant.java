package mobiusws.callsLoader;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class Assistant implements Runnable {

	private CLContext clContext;
	private LinkedBlockingQueue<String> readyProcessHourQ;
	final private HashMap<String,Integer> filesGo = new HashMap<String,Integer>();
	private int fileSize ;
	private ConcurrentHashMap<String, Integer> feedingRecorder;
	
	public Assistant(LinkedBlockingQueue<String> readyProcessHourQ, CLContext clContext, ConcurrentHashMap<String, Integer> feedingRecorder){
		this.clContext = clContext;
		this.readyProcessHourQ = readyProcessHourQ;
		this.feedingRecorder = feedingRecorder;
	}
	
	
	public void run() {
		
		while(true){
			
			if(readyProcessHourQ.isEmpty()){
				
				String localCsvP =  clContext.getConf().getLocalCsvPath();
				String files[] = new File(localCsvP).list();
				if(files == null){
					
					App.logger.error("--------------- Local csv path  " +localCsvP + " is not dir.please check!---------------"  );
					break;
					
				}else if(files.length != 0){
								
					
					for(String fileName: files){
				    	String fileDate = fileName.toString().substring(0, 10);				    	
				    	String number =fileName.substring(fileName.lastIndexOf('_') +1, fileName.lastIndexOf('.'));
				    	Integer lastestCountNumber = Integer.parseInt(number);
				    	
				    	if(!filesGo.containsKey(fileDate)){
				    		filesGo.put(fileDate, lastestCountNumber);
   		
				    	}else{
				    		if(filesGo.get(fileDate) < lastestCountNumber)filesGo.put(fileDate, lastestCountNumber);
				    	} 	
					}
					
					if(fileSize == files.length){
						
						
						String readyGo[] = new String[filesGo.size()];
						Set<Entry<String,Integer>> set = filesGo.entrySet();
						Iterator<Entry<String,Integer>> ite  = set.iterator();
						int count = 0;
						while(ite.hasNext()){
							Entry<String,Integer> ent = ite.next();
							readyGo[count] = ent.getKey() + "_" + ent.getValue();
							count++;
						}
						for(String redStr : readyGo){
							App.logger.info("~~~~~~~~~~~~~~~~~~~~~~~ assistant will offer hour "+ redStr+ "  to queue for  loading ~~~~~~~~~~~~~~~~~~~~~~~");
							readyProcessHourQ.offer(redStr);
							String hour = redStr.split("_")[0];
							App.logger.info("~~~~~~~~~~~~~~~~~~~~~~~ assistant will delete " + hour +  " in feeding recorder ~~~~~~~~~~~~~~~~~~~~~~~");
							feedingRecorder.remove(hour);
						}
						
						filesGo.clear();
						fileSize = 0;
						
						try {
							Thread.sleep(12*3600 * 1000l);
						} catch (InterruptedException e) {
							ExceptionToolkit.exceptionToLogWithMsg(e, " Assistant has been interrupted. ");
						}
					}else{
						fileSize = files.length;
						try {
							Thread.sleep(30*60 * 1000l);
						} catch (InterruptedException e) {
							ExceptionToolkit.exceptionToLogWithMsg(e, " Assistant double check has been interrupted. ");
						}
					}
					
					
					
				}else{
					sleep();
				}
				
				
				
			}else{
				sleep();
			}
			
			
		}
		
	}

	
	private void sleep(){
		fileSize =0;
		filesGo.clear();
		
		try {
			Thread.sleep(10*60 * 1000l);
		} catch (InterruptedException e) {
			ExceptionToolkit.exceptionToLogWithMsg(e, " Assistant idle has been interrupted. ");
		}
	}
}
