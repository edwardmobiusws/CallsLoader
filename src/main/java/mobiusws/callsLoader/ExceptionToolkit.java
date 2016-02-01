package mobiusws.callsLoader;


public class ExceptionToolkit {

	/**
	 * toLog
	 * 
	 * @param e
	 */
	public static void exceptionToLog(Exception e) {
		StackTraceElement[] messages = e.getStackTrace();
		int length = messages.length;
		App.logger.error(e.getMessage());
		for (int i = 0; i < length; i++) {
			App.logger.error("Exception StackTrace " + i + " : " + messages[i].toString());
			
		}
		
		
	}
	
	public static void exceptionToLogWithMsg(Exception e, String mes){
		App.logger.error(mes, e);
		if(e != null)exceptionToLog(e);
	}

}
