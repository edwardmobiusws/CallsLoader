package mobiusws.callsLoader;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CLContext {

	private LoaderConfig conf;
	private FileSystem fs;
	private long processingDate;
	
	public CLContext(LoaderConfig conf){
		this.conf = conf;
	}
	


	public boolean init(){
		
		App.logger.info("+++ init clcontext start!++++");

    	Configuration hdConf = new Configuration();
    	hdConf.addResource(new Path(conf.getHdCoreSiteXmlPath()));
    	hdConf.addResource(new Path(conf.getHdfsSiteXmlPath()));
    	
    	// bug "hadoop No FileSystem for scheme: file" when using maven-assembly
    	hdConf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    	hdConf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName() );
    	
    	
    	try {
			FileSystem fileSys = FileSystem.get(hdConf);
			setFs(fileSys);
		} catch (IOException e) {
			ExceptionToolkit.exceptionToLogWithMsg(e, "Get hadoop file system failed!");
			return false;
		}
    	
    	try {
    		Path processingDatePath = new Path(conf.getLoadingDateFilePath());
			if(fs.exists(processingDatePath)){
				FSDataInputStream fsdis = fs.open(processingDatePath);
				long date  = fsdis.readLong();
				setProcessingDate(date);
				fsdis.close();
				
			}else{
				FSDataOutputStream fsdos  = fs.create(processingDatePath);
				fsdos.writeLong(0l);
				setProcessingDate(0l);
				fsdos.close();
				
			}
			
		} catch (IllegalArgumentException e) {
			ExceptionToolkit.exceptionToLogWithMsg(e, "Path " + conf.getLoadingDateFilePath() + " has a problem to be created");
			return false;
		} catch (IOException e) {
			ExceptionToolkit.exceptionToLogWithMsg(e, "IOException thrown by writing long to path " + conf.getLoadingDateFilePath());
			return false;
		}

    	App.logger.info("+++ init clcontext end!++++");
    	return true;
	}

	public LoaderConfig getConf() {
		return conf;
	}

	public void setConf(LoaderConfig conf) {
		this.conf = conf;
	}




	public FileSystem getFs() {
		return fs;
	}


	public void setFs(FileSystem fs) {
		this.fs = fs;
	}


	public long getProcessingDate() {
		return processingDate;
	}


	public void setProcessingDate(long processingDate) {
		this.processingDate = processingDate;
	}


	
	
}
