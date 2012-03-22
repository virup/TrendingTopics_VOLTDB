package trendingtopics.procedures;

import org.voltdb.*;
import java.util.Date;

@ProcInfo(

	singlePartition= false
)



public class purge extends VoltProcedure {
	public final long TIMEOUT = 5 * 60 * 1000; // 5 mins in milliseconds
	public final SQLStmt sqlTop = new SQLStmt(
		"DELETE FROM WORDCOUNT WHERE (? - time ) >= " + TIMEOUT + ";" 
	);


	public VoltTable run()
        {
	    Date now = new Date();
    	    voltQueueSQL(sqlTop, now.getTime());
    	    return voltExecuteSQL()[0];
	}
};
	
