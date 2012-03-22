package trendingtopics.procedures;
import org.voltdb.*;

@ProcInfo(

	singlePartition= false
)


public class initialize extends VoltProcedure {

	public final SQLStmt sqlTop = new SQLStmt(
		"INSERT INTO STATS VALUES(?, 1);"
	);


	public VoltTable run()
        {
    	    voltQueueSQL(sqlTop, "read");
	    voltQueueSQL(sqlTop, "write");
            voltQueueSQL(sqlTop, "update");
    	    return voltExecuteSQL()[0];
	}
};

