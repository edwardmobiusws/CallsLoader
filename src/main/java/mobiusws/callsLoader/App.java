package mobiusws.callsLoader;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Loading csv files into hive table CALLS.
 * @author Edward
 *
 */
public class App {
	
	
	private final static String  MINIUM_LOADER_THREAD_NUMBER = "minLoaderThreadNum";
	private final static String  HIVE_META_URIS = "hiveMetastorUris";
	private final static String  HIVE_WAREHOUSE_DIR = "hiveMsWhDir";
	private final static String  LOCAL_CSV_PATH = "localCsvPath";
	private final static String  BACKUP_CSV_PATH = "backupCsvPath";
	private final static String  ERROR_CSV_PATH = "errorCsvPath";
	private final static String  OPERATOR = "operator";
	private final static String  DBNAME = "database";
	private final static String  HDFS_CSV_PATH = "hdfsCsvPath";
	private final static String  HD_CORE_SITE_XML_PATH = "hdCoreSiteXmlPath";
	private final static String  HDFS_SITE_XML_PATH = "hdfsSiteXmlPath";
	private final static String  LOADING_DATE_FILE_PATH = "loadingDateFilePath";
	private final static String  WORK_CSV_PATH = "workCsvPath";
	private final static String  FILES_NUMBER_1_HOUR = "filesNumber1hour";
	private final static String	 HIVE_JDBC_SQL="hiveJDBCsql";
	
	private static final Pattern SPACE = Pattern.compile(" ");
	public static final Logger logger = LogManager.getLogger(App.class);
	
    public static void main( String[] args )
    {
    	
    	LoaderConfig loaderConf = new LoaderConfig();
    	
    	String home  = System.getProperty("callsLoaderHome");
    	String confPath = (home==null || home.length()==0)?"/opt/callsLoader":home;
    	
    	RandomAccessFile proFile;
    	Long len= new Long(0);
    	FileChannel fc = null;
		try {
			proFile = new RandomAccessFile(confPath + File.separator +  "callsLoader.properties", "r");
			len  = proFile.length();
			fc = proFile.getChannel();
			
			ByteBuffer bf  = ByteBuffer.allocate(len.intValue());
			fc.read(bf);
			
			bf.flip();
			
			BufferedReader bre = new BufferedReader(new StringReader(new String(bf.array())));
			 List<String> lines = new ArrayList<String>();
			String line  = null;
			while ((line  = bre.readLine()) != null){
				lines.add(line);
			}
			loaderConf = getLoaderConfig(lines);
			
			
	        logger.info("minLoaderThreadNum:" + loaderConf.getMinLoaderThreadNum() 
			+ ", hiveMetastorUris: " + loaderConf.getHiveMetastorUris()
			+ ", hiveMsWhDir: " + loaderConf.getHiveMsWhDir()
			+ ", localCsvPath: " + loaderConf.getLocalCsvPath()
			+ ", backupCsvPath: " + loaderConf.getBackupCsvPath()
			+ ", errorCsvPath: " + loaderConf.getErrorCsvPath()
			+ ", operator: " + loaderConf.getOperator()
			+ ", database: "+ loaderConf.getDatabase()
			+ ", hdfsCsvPath: "+ loaderConf.getHdfsCsvPath()
			+ ", hdfsCoreSiteXmlPath: " + loaderConf.getHdCoreSiteXmlPath()
			+ ", hdfsSiteXmlPath: " + loaderConf.getHdfsSiteXmlPath()
			+ ", loadingDateFilePath: " + loaderConf.getLoadingDateFilePath()
			+ ", workCsvPath: " + loaderConf.getWorkCsvPath()
			+ ", filesNumber1hour: " + loaderConf.getFilesNumber1Hour());

			bf.clear();
			fc.close();
			proFile.close();
			
		} catch (FileNotFoundException e) {
			logger.error("callsLoader.properties doesn't exist, app will exit!");
			System.exit(-1);
		}catch (IOException e2) {
			logger.error("Reading callsLoader.properties failed with ioe. " + e2.getMessage());
			System.exit(-1);
		}
		
    	
        ConcurrentHashMap<String, Integer> feedingRecorder = new  ConcurrentHashMap<String, Integer>();
        LinkedBlockingQueue<String> readyProcessHourQ = new LinkedBlockingQueue<String>();
        
        
        String csvPath = loaderConf.getLocalCsvPath();
		Path dir = Paths.get(csvPath);
    	
		WatchService watcher = null;
		try {
			watcher = FileSystems.getDefault().newWatchService();
			dir.register(watcher, ENTRY_CREATE);
		} catch (IOException e) {
			ExceptionToolkit.exceptionToLogWithMsg(e,"++++: new watch service for path "+ csvPath + " failed.");
		}
		if(watcher == null) {
			System.exit(-1);
		}
	    
		
		int minLoaderThreadNum = loaderConf.getMinLoaderThreadNum();
		//ExecutorService executor = Executors.newFixedThreadPool(minLoaderThreadNum);
		CLContext clContext = new CLContext(loaderConf);
		
		boolean initSuccessful = clContext.init();
		if(!initSuccessful) return ;
	
		ReentrantLock locker = new ReentrantLock();
		
		LoaderThreads lt = new LoaderThreads(clContext, locker, readyProcessHourQ);
		for(int i = 0;i< minLoaderThreadNum; i++){
			Thread tt  = new Thread(lt);
			tt.start();			
		}
		// assistant help to deal with files not been proccessed because of disorder files incoming not by time.
		new Thread(new Assistant(readyProcessHourQ, clContext)).start();
		
	
        for (;;) {

            // wait for key to be signaled
            WatchKey key;
            try {
                key = watcher.take();
            } catch (InterruptedException x) {
                return;
            }

            for (WatchEvent<?> event: key.pollEvents()) {
           	
                WatchEvent.Kind kind = event.kind();

                if (kind == OVERFLOW) {
                    WatchEvent<Path> ev = (WatchEvent<Path>)event;
                    Path filename = ev.context();
                    logger.info("Warnning ,watch service over flow event occurs, file  " +  filename + " ignored!");
                	
                    continue;
                }

                //The filename is the context of the event.
                WatchEvent<Path> ev = (WatchEvent<Path>)event;
                Path filename = ev.context();
                
                
                if( filename.toString().endsWith("csv")){
                	
                	IncomingRecorder reco = new IncomingRecorder(feedingRecorder, filename.toString(), readyProcessHourQ);
                	new Thread(reco).start();
                }

            }
            	          
            boolean valid = key.reset();
            if (!valid) {
                    break;
            }

        }
    	
    }
    
    static LoaderConfig getLoaderConfig(List<String> lines){
        LoaderConfig loaderConf = new LoaderConfig();
        
        for(String line : lines){			
			String[] tok = SPACE.split(line);
			if(tok[0].equals(MINIUM_LOADER_THREAD_NUMBER)){
				if(tok.length > 1)loaderConf.setMinLoaderThreadNum(Integer.parseInt(tok[1]));
				
			}else if(tok[0].equals(HIVE_META_URIS)){
				if(tok.length > 1)loaderConf.setHiveMetastorUris(tok[1]);
				
			}else if(tok[0].equals(HIVE_WAREHOUSE_DIR)){
				if(tok.length > 1)loaderConf.setHiveMsWhDir(tok[1]);
				
			}else if(tok[0].equals(LOCAL_CSV_PATH)){
				if(tok.length > 1)loaderConf.setLocalCsvPath(tok[1]);
				
			}else if(tok[0].equals(BACKUP_CSV_PATH)){
				if(tok.length > 1)loaderConf.setBackupCsvPath(tok[1]);
				
			}else if(tok[0].equals(ERROR_CSV_PATH)){
				if(tok.length > 1)loaderConf.setErrorCsvPath(tok[1]);
				
			}else if(tok[0].equals(OPERATOR)){
				if(tok.length > 1)loaderConf.setOperator(tok[1]);
				
			}else if(tok[0].equals(DBNAME)){
				if(tok.length > 1)loaderConf.setDatabase(tok[1]);
				
			}else if(tok[0].equals(HDFS_CSV_PATH)){
				if(tok.length > 1)loaderConf.setHdfsCsvPath(tok[1]);
				
			}else if(tok[0].equals(HD_CORE_SITE_XML_PATH)){
				if(tok.length > 1)loaderConf.setHdCoreSiteXmlPath(tok[1]);
				
			}else if(tok[0].equals(HDFS_SITE_XML_PATH)){
				if(tok.length > 1)loaderConf.setHdfsSiteXmlPath(tok[1]);
				
			}else if(tok[0].equals(LOADING_DATE_FILE_PATH)){
				if(tok.length > 1)loaderConf.setLoadingDateFilePath(tok[1]);
			}else if(tok[0].equals(WORK_CSV_PATH)){
				if(tok.length > 1)loaderConf.setWorkCsvPath(tok[1]);
				
			}else if(tok[0].equals(FILES_NUMBER_1_HOUR)){
				if(tok.length > 1)loaderConf.setFilesNumber1Hour(Integer.parseInt(tok[1]));
			}else if(tok[0].equals(HIVE_JDBC_SQL)){
				if(tok.length > 1)loaderConf.setHiveJDBCsql(tok[1]);
			}
			
			
        }
        return loaderConf;
    }
   
}
