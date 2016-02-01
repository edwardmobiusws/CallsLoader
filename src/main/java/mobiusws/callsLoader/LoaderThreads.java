package mobiusws.callsLoader;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;

import static java.nio.file.StandardCopyOption.*;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FSDataOutputStream;


public class LoaderThreads implements Runnable {

	private CLContext clcontext;
	private ReentrantLock locker;	
	private LinkedBlockingQueue<String> readyProcessHourQ ;
	
	
	public LoaderThreads( CLContext context,ReentrantLock locker,LinkedBlockingQueue<String> readyProcessHourQ){

		this.clcontext = context;
		this.locker = locker;
		this.readyProcessHourQ = readyProcessHourQ;
	}
	
	
	class GenerateHourFile implements Runnable{

		private long startIndex;
		private long endIndex;
		private FileChannel fileChannel;
		private ArrayList<File> files;
		
		public GenerateHourFile(FileChannel fileChannel, long startIndex, long endIndex, ArrayList<File> files ) {
			this.fileChannel = fileChannel;
			this.startIndex = startIndex;
			this.endIndex = endIndex;
			this.files = files;
		}
		
		private  void unmap(final MappedByteBuffer mappedByteBuffer) {
		        try {
		            if (mappedByteBuffer == null) {
		                return;
		            }
		             
		            mappedByteBuffer.force();
		            AccessController.doPrivileged(new PrivilegedAction<Object>() {
		                @SuppressWarnings("restriction")
		                public Object run() {
		                    try {
		                        Method getCleanerMethod = mappedByteBuffer.getClass()
		                                .getMethod("cleaner", new Class[0]);
		                        getCleanerMethod.setAccessible(true);
		                        sun.misc.Cleaner cleaner = 
		                                (sun.misc.Cleaner) getCleanerMethod
		                                    .invoke(mappedByteBuffer, new Object[0]);
		                        cleaner.clean();
		                         
		                    } catch (Exception e) {
		                    	ExceptionToolkit.exceptionToLog(e);
		                    }
		                  
		                    return null;
		                }
		            });
		 
		        } catch (Exception e) {
		        	ExceptionToolkit.exceptionToLog(e);
		        }
		    }

		public  void run()  {

			FileChannel sourChan  = null;
			ByteBuffer buf = null;
			MappedByteBuffer mbb = null;
			
			try{
				long start = startIndex;
				
				for(int i = 0; i<files.size() ; i++) {

				 File sourceFile  = files.get(i);
				 App.logger.info(" Start Reading file " + sourceFile.getName() + "!");	 
				 int size = (int)sourceFile.length();
				 mbb = fileChannel.map(MapMode.READ_WRITE, start, size);
				 App.logger.info(" mapping fileChannel finished ! from size " + start + " to size " + (start+size) + " for file " +  sourceFile.getName() + "!");	
				 
				 RandomAccessFile raf = new RandomAccessFile(sourceFile, "r");
				 sourChan = raf.getChannel();
				 buf = ByteBuffer.allocate(size);
				 sourChan.read(buf);
				 App.logger.info(" Reading file " + sourceFile.getName() + " finised!");	 
				 

				 buf.flip();
				 mbb.put(buf);
				 buf.clear();
				 sourChan.close();
				 
				 start = start + size;
				 
				 raf.close();
				}
				
				if( start != endIndex)App.logger.info("error ,start : " + start+ ", endIndex: " + endIndex);
				
			}catch(IOException e){
				ExceptionToolkit.exceptionToLog(e);
			}finally{
				if(mbb!= null)unmap(mbb);
			}
			
		}
		
	}
	
	public void run(){
		int filesNumber1Hour = clcontext.getConf().getFilesNumber1Hour();
		
		while(true){			
			try {
				
				String needProcessH  = readyProcessHourQ.take();
				
				String[] res = needProcessH.split(IncomingRecorder.sperator);
				String date= res[0];
				int count =  Integer.parseInt(res[1]);
				App.logger.info("+++++ start to process hour " + date + " ++++++++++++++");
				
				ArrayList<String> theHourFiles = new ArrayList<String>();
				for(int i=0;i <=count;i++){
					if(new File(clcontext.getConf().getLocalCsvPath() + File.separator + date+"_"+i+ ".csv").exists()){
						theHourFiles.add(date+"_"+i+ ".csv");
					}
				}
				
				int theHourFilesSize = theHourFiles.size() ;		
				App.logger.info("theHourFiles size "  + theHourFilesSize);
				
				if(theHourFilesSize > 0){
					insertLoaderStatus(date);
					boolean hasError = false;
					int mod =  theHourFiles.size() / filesNumber1Hour;
					if(mod ==0) {
						// that means feeding files is less than the number of files we want , just move them to work ,and do loader.
						for (String str: theHourFiles){
							String localCsv = clcontext.getConf().getLocalCsvPath() + File.separator + str;
							String workCsv =  clcontext.getConf().getWorkCsvPath() +  File.separator + str;
							if(!moveFile(localCsv, workCsv))hasError = true;
							if(!doLoader(str))hasError = true;
						}
						
					}else{
						App.logger.info("mod : " + mod);
						for(int i = 0; i<filesNumber1Hour; i ++){
							long startIndex = 0;
							
							String desPath = clcontext.getConf().getWorkCsvPath()+ File.separator + date + "_" + i + ".csv";
							File desFile = new File(desPath);
							if(desFile.exists())desFile.delete();
							RandomAccessFile rafile = new RandomAccessFile(desFile, "rw");
							FileChannel  fileChannel = rafile.getChannel();
							
							int index = i*mod;
							int limit = 0;
							if(i ==  filesNumber1Hour -1){
								limit = theHourFiles.size();
							}else{
								limit = index + mod;
							}	
							long size = 0;
							ArrayList<File> ll = new ArrayList<File>(); 
							for(int n = index; n< limit; n ++){
								String fileName = theHourFiles.get(n);
								File  file  = new File(clcontext.getConf().getLocalCsvPath() + File.separator + fileName);
								size = size + file.length();								
								ll.add(file);
							}
							App.logger.info("will new GenerateHourFile " );
							GenerateHourFile ghf  = new GenerateHourFile(fileChannel, startIndex, startIndex + size, ll);
							Thread gen =new  Thread(ghf);
							gen.start();
							gen.join();
							fileChannel.close();
							rafile.close();
							App.logger.info("generating file " + desPath + " finished.");
							if(!doLoader(date + "_" + i + ".csv"))hasError= true;
							
							for(File file : ll){
								//Files.deleteIfExists(Paths.get(file.getPath()));
								moveFile(file.getPath(), clcontext.getConf().getBackupCsvPath() + File.separator + file.getName());
							}
							ll.clear();
							
						}
					}
					
					updateLoaderStatus(date, hasError);
				}		
				App.logger.info("+++++ processing hour " + date + " finished! ++++++++++++++");
			} catch (IOException e) {
				ExceptionToolkit.exceptionToLog(e);
			} catch (InterruptedException e) {
				ExceptionToolkit.exceptionToLog(e);
			}
		}	
	}
	
	private void insertLoaderStatus(String hour){
    	Connection con= null;
    	Statement stmt = null;
    	String date  = hour.substring(0, 8);
	    try {
	        Class.forName("org.apache.hive.jdbc.HiveDriver");
	        con = DriverManager.getConnection(clcontext.getConf().getHiveJDBCsql(), "hadoop", "");        
		    stmt = con.createStatement();
		      
		    //insert loader_status
		    
		    String now = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime());
		    
		    ResultSet rs = stmt.executeQuery("select traffic_time from decode_info  where traffic_time='" + hour + "'");	    
		    if(!rs.next()){
		    	String hql = " insert into table decode_info partition (traffic_date='" + date + "') values ('" +hour +"',1,1,1,1,1, '1111111111','" + now+ "','" +now +"')";
		    	App.logger.info(hql);
		    	stmt.execute(hql);
		    }else{
		    	String hql = "update decode_info set loader_process_state =1," + " update_time ='"+ now +"' where traffic_time='"+ hour  + "'";
		    	App.logger.info(hql);
		    	stmt.executeUpdate(hql);
		    }	    
	    }catch (ClassNotFoundException e) {
	        ExceptionToolkit.exceptionToLog(e);
	        System.exit(1);
	      } catch (SQLException e) {
	    	  ExceptionToolkit.exceptionToLogWithMsg(e, "set loader status for hour " +  hour + " failed!" );
	      }finally {
				try {
					if(stmt != null)stmt.close();
					if(con != null)con.close();
				} catch (SQLException e) {
					ExceptionToolkit.exceptionToLog(e);
				}
			}
	}
	
	private void updateLoaderStatus(String hour,boolean hasError){
    	Connection con= null;
    	Statement stmt = null;
    	int flag = 2;
	    try {
	        Class.forName("org.apache.hive.jdbc.HiveDriver");
	        con = DriverManager.getConnection(clcontext.getConf().getHiveJDBCsql(), "hadoop", "");        
		    stmt = con.createStatement();
		    String now = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime());
	    	if(hasError)flag = 9;
	    	String loader_status_sql = "update decode_info set loader_process_state =" +flag+", update_time ='"+ now +"' where traffic_time='"+ hour  + "'";
	    	App.logger.info("loader_status_sql =" + loader_status_sql);
		    stmt.executeUpdate(loader_status_sql);
    
	    }catch (ClassNotFoundException e) {
	        ExceptionToolkit.exceptionToLog(e);
	        System.exit(1);
	      } catch (SQLException e) {
	    	  ExceptionToolkit.exceptionToLogWithMsg(e, "set  loader_process_state= "+flag  + " for hour " +  hour + " failed when finish loading!" );
	      }finally {
			try {
				if(stmt != null)stmt.close();
				if(con != null)con.close();
			} catch (SQLException e) {
				ExceptionToolkit.exceptionToLog(e);
			}
		}
	}
	
	private boolean doLoader(String fileName){
		boolean hasError = false;
    	String fileDate = fileName.toString().substring(0, 10);
    	String dateStr = fileDate.substring(0, 4) + "-"+ fileDate.substring(4, 6) + "-" + fileDate.substring(6, 8);
    	String hourStr = fileDate.substring(8, 10);
    	
    	String source = clcontext.getConf().getWorkCsvPath() + File.separator + fileName;
    	App.logger.info(source +" length: " +new File(source).length() + ", it will be moved to hdfs!");
    	if(!putIntoHDFS(clcontext.getConf().getWorkCsvPath() + File.separator + fileName, clcontext.getConf().getHdfsCsvPath()))
    		hasError = true;

    	Connection con= null;
    	Statement stmt = null;
	    try {
	        Class.forName("org.apache.hive.jdbc.HiveDriver");
	    
	        con = DriverManager.getConnection(clcontext.getConf().getHiveJDBCsql(), "hadoop", "");        
		    stmt = con.createStatement();    
		    stmt.execute("set hive.exec.dynamic.partition.mode=nonstrict");
		    stmt.execute("CREATE TABLE IF NOT EXISTS calls_no_dup("
	  		  		+ " sw_id int, s_imsi string, s_imei string, s_ci int, s_lac int, trunk_in string, trunk_out string, term_cause int, term_reason int,"
	  		  		+ " ss_code int, charge_indicator int, call_id bigint, o_msisdn string, call_start_time string, duration int, o_call_id bigint,"
	  		  		+ " call_type int, call_end_time string, s_msisdn string)"
	  		  		+ " PARTITIONED BY(call_date string, call_hour string)"
	  		  		+ " CLUSTERED BY(s_msisdn) sorted by(call_start_time) INTO 32 buckets ROW FORMAT DELIMITED FIELDS TERMINATED BY ','");
		    
		    
  		  //  String loadSql = "LOAD DATA  LOCAL INPATH '" +  clcontext.getConf().getWorkCsvPath() + File.separator + fileName+ "' INTO TABLE calls_no_dup"
  		  // 		+ " PARTITION (call_date='" + dateStr + "',call_hour='"+ hourStr +"')";
		    String loadSql = "LOAD DATA INPATH 'hdfs://mars02-db01" + File.separator + clcontext.getConf().getHdfsCsvPath() + File.separator 
		      + fileName  + "'" +   "  INTO TABLE calls_no_dup " +" PARTITION (call_date='" + dateStr + "',call_hour='"+ hourStr +"')";
  		   
		    App.logger.info( "~~~~sql = " + loadSql);
		    stmt.execute(loadSql);
	     
	      } catch (ClassNotFoundException e) {
	        ExceptionToolkit.exceptionToLog(e);
	        System.exit(1);
	      } catch (SQLException e) {
	    	  ExceptionToolkit.exceptionToLogWithMsg(e, "++++++++++ loading " 
	    			  		+ clcontext.getConf().getWorkCsvPath() + File.separator + fileName +" error :" +e.getMessage());
	    	  hasError = true;	    	  
	      } finally {
			    try {
					if(stmt != null)stmt.close();
					if(con != null)con.close();
				} catch (SQLException e) {
					ExceptionToolkit.exceptionToLog(e);
				}			   
		  }
    	
	    putIntoHDFS(clcontext.getConf().getWorkCsvPath() + File.separator + fileName, clcontext.getConf().getHdfsCsvPath());

    	String destination = null;
    	if(hasError){
    		 destination = clcontext.getConf().getErrorCsvPath() + File.separator + fileName;	 
    	}else{
    		 destination = clcontext.getConf().getBackupCsvPath() + File.separator + fileName;
    		 
    	}
		moveFile(source, destination);
		long fileDateLong  = Long.parseLong(fileDate);
		
		if(fileDateLong > clcontext.getProcessingDate())updateLoadingDate(fileDateLong);
		
		App.logger.info("++++ running loader for file " + fileName + " process finished.");
		return !hasError;
	}

	private boolean moveFile(String source, String destination){
		Path src = null;
		Path des = null;
		boolean successful = true;
		try {
			src  = Paths.get(source);
			des  = Paths.get(destination);
			Files.move(src, des, REPLACE_EXISTING);
			App.logger.info("Moving file to " + des.toAbsolutePath() + " finished!");
			App.logger.info(des.toAbsolutePath() + " exist : " + new File(destination).exists());
		} catch (IOException e) {
			
			ExceptionToolkit.exceptionToLogWithMsg(e, "Move file using Files.move from " + src + " to " + des + " failed.");
			successful = false;
		}
		
		if(!successful){
			 File file = new File(source);   
			 File newfile = new File(destination); 
			 
			 try{
				 FileUtils.copyFile(file, newfile);
				 file.delete();
				 App.logger.info("Move file  successfully by using copy and delete . from " + src + " to " + des + " .");
				 successful = true;
			 }catch(IOException e){
				 ExceptionToolkit.exceptionToLog(e);
			 }
			 
		}
		return successful;
	}
	
	private void updateLoadingDate(long fileDateLong){
		locker.lock();
		
		try{
			
			// other thread properly updated  loading date.  check it again.  
			if(fileDateLong > clcontext.getProcessingDate()){
				clcontext.setProcessingDate(fileDateLong);
				try {
					org.apache.hadoop.fs.Path proDatePath = new org.apache.hadoop.fs.Path(clcontext.getConf().getLoadingDateFilePath()); 
					clcontext.getFs().delete(proDatePath , true);
					FSDataOutputStream fsdos = clcontext.getFs().create(proDatePath);
					fsdos.writeLong(fileDateLong);
					fsdos.close();
					
					App.logger.info("Loading date " + fileDateLong +" has been updated into file "+  clcontext.getConf().getLoadingDateFilePath());
				} catch (IOException e) {
					ExceptionToolkit.exceptionToLogWithMsg(e, "updating date " + fileDateLong +" into file "+  clcontext.getConf().getLoadingDateFilePath() + " failed. ");
				}
			}
			
		}finally{
			locker.unlock();
		}
	}
	
	private boolean putIntoHDFS(String localCsvPath, String destination){
		boolean suc = true;
		org.apache.hadoop.fs.Path desPath = new org.apache.hadoop.fs.Path(destination);
		org.apache.hadoop.fs.Path locPath = new org.apache.hadoop.fs.Path(localCsvPath);
		
		try {
			
			if(!clcontext.getFs().exists(desPath)){
				clcontext.getFs().mkdirs(desPath);
			}
			clcontext.getFs().copyFromLocalFile(locPath, desPath);
			
			
		} catch (IOException e) {
			
			ExceptionToolkit.exceptionToLogWithMsg(e, "copy local file to hdfs from " + localCsvPath + " to " + destination + " failed.");
			suc = false;
		}
		return suc;
	}

	
	
}
