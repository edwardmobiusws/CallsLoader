package mobiusws.callsLoader;

import java.io.Serializable;

class LoaderConfig implements Serializable {

	private int minLoaderThreadNum;
	private String hiveMetastorUris;
	private String hiveMsWhDir;
	private String localCsvPath;
	private String backupCsvPath;
	private String errorCsvPath;
	private String operator;
	private String database;
	private String hdfsCsvPath;
	private String hdCoreSiteXmlPath;
	private String hdfsSiteXmlPath;
	private String loadingDateFilePath;
	private String workCsvPath;
	private int filesNumber1Hour;
	private String hiveJDBCsql;
	
	public int getMinLoaderThreadNum() {
		return minLoaderThreadNum;
	}
	public void setMinLoaderThreadNum(int minLoaderThreadNum) {
		this.minLoaderThreadNum = minLoaderThreadNum;
	}
	public String getHiveMetastorUris() {
		return hiveMetastorUris;
	}
	public void setHiveMetastorUris(String hiveMetastorUris) {
		this.hiveMetastorUris = hiveMetastorUris;
	}
	public String getHiveMsWhDir() {
		return hiveMsWhDir;
	}
	public void setHiveMsWhDir(String hiveMsWhDir) {
		this.hiveMsWhDir = hiveMsWhDir;
	}
	public String getLocalCsvPath() {
		return localCsvPath;
	}
	public void setLocalCsvPath(String localCsvPath) {
		this.localCsvPath = localCsvPath;
	}
	public String getBackupCsvPath() {
		return backupCsvPath;
	}
	public void setBackupCsvPath(String backupCsvPath) {
		this.backupCsvPath = backupCsvPath;
	}
	public String getErrorCsvPath() {
		return errorCsvPath;
	}
	public void setErrorCsvPath(String errorCsvPath) {
		this.errorCsvPath = errorCsvPath;
	}
	public String getOperator() {
		return operator;
	}
	public void setOperator(String operator) {
		this.operator = operator;
	}
	public String getDatabase() {
		return database;
	}
	public void setDatabase(String database) {
		this.database = database;
	}
	public String getHdfsCsvPath() {
		return hdfsCsvPath;
	}
	public void setHdfsCsvPath(String hdfsCsvPath) {
		this.hdfsCsvPath = hdfsCsvPath;
	}
	public String getHdCoreSiteXmlPath() {
		return hdCoreSiteXmlPath;
	}
	public void setHdCoreSiteXmlPath(String hdCoreSiteXmlPath) {
		this.hdCoreSiteXmlPath = hdCoreSiteXmlPath;
	}
	public String getHdfsSiteXmlPath() {
		return hdfsSiteXmlPath;
	}
	public void setHdfsSiteXmlPath(String hdfsSiteXmlPath) {
		this.hdfsSiteXmlPath = hdfsSiteXmlPath;
	}
	public String getLoadingDateFilePath() {
		return loadingDateFilePath;
	}
	public void setLoadingDateFilePath(String loadingDateFilePath) {
		this.loadingDateFilePath = loadingDateFilePath;
	}
	public String getWorkCsvPath() {
		return workCsvPath;
	}
	public void setWorkCsvPath(String workCsvPath) {
		this.workCsvPath = workCsvPath;
	}
	public Integer getFilesNumber1Hour() {
		return filesNumber1Hour;
	}
	public void setFilesNumber1Hour(Integer filesNumber1Hour) {
		this.filesNumber1Hour = filesNumber1Hour;
	}
	public String getHiveJDBCsql() {
		return hiveJDBCsql;
	}
	public void setHiveJDBCsql(String hiveJDBCsql) {
		this.hiveJDBCsql = hiveJDBCsql;
	}
	
	
	
}
