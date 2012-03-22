package trendingtopics.procedures;
import org.voltdb.*;

@ProcInfo(

	singlePartition= false

)


public class gettopics extends VoltProcedure {

	public final SQLStmt sqlTop = new SQLStmt(
		"SELECT word, wcount FROM WORDCOUNT ORDER BY wcount DESC LIMIT 10 ;"
	);


	public VoltTable run()
        {
    	    voltQueueSQL(sqlTop);
    	    return voltExecuteSQL()[0];
	}
};

