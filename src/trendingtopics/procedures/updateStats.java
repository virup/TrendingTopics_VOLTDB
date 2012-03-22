package trendingtopics.procedures;
import org.voltdb.*;

@ProcInfo(

	singlePartition= false
)


public class updateStats extends VoltProcedure {

	public final SQLStmt sqlStats = new SQLStmt(
		"UPDATE STATS SET opscount=?  WHERE operation=?;" 
	);

	public final SQLStmt sqlReadStats = new SQLStmt(
		"select * from STATS;" 
	);



	public VoltTable[] run(int read, int write, int update)
	{	
		voltQueueSQL(sqlReadStats);
		VoltTable[] vt  = voltExecuteSQL();

		read = read + (int)(vt[0].fetchRow(0).getLong(1));
		write = write + (int)(vt[0].fetchRow(1).getLong(1));
		update = update + (int)(vt[0].fetchRow(2).getLong(1));

		voltQueueSQL(sqlStats, read+3, "read");
		voltQueueSQL(sqlStats, write, "write");
		voltQueueSQL(sqlStats, update + 3, "update");
		return voltExecuteSQL();
		
	}
};

