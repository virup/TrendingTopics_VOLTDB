package trendingtopics.procedures;
import org.voltdb.*;

@ProcInfo(

	singlePartition= false
)


public class readstats extends VoltProcedure {

	public final SQLStmt sqlStats = new SQLStmt(
		"SELECT * FROM STATS;"
	);	
	public final SQLStmt sqlRefresh = new SQLStmt(
		"UPDATE STATS SET opscount = 0;"
	);


	public VoltTable run()
        {
    	    voltQueueSQL(sqlStats);
	    voltQueueSQL(sqlRefresh);
    	    return voltExecuteSQL()[0];
	}
};

