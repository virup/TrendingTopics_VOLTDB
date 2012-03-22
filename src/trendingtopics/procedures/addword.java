package trendingtopics.procedures;
import org.voltdb.*;
import java.util.Date;

@ProcInfo(
	singlePartition= false
)


public class addword extends VoltProcedure {

	public final SQLStmt sqlInsert = new SQLStmt(
		"INSERT INTO WORDCOUNT VALUES (?, ?, ?);"
	);
	
	public final SQLStmt sqlExists = new SQLStmt(
		"SELECT * from WORDCOUNT WHERE word = ?;" 
	);

	public final SQLStmt sqlUpdate = new SQLStmt(
		"UPDATE WORDCOUNT SET wcount=?, time=? WHERE word=?;" 
	);

	public final SQLStmt sqlStats = new SQLStmt(
		"UPDATE STATS SET opscount=opscount + ?  WHERE operation=?;" 
	);

	public final SQLStmt sqlReadStats = new SQLStmt(
		"SELECT * FROM STATS;" 
	);
	


	public void countStat(int read, int write, int update)
	{	

		voltQueueSQL(sqlStats, read+3, "read");
		voltQueueSQL(sqlStats, write, "write");
		voltQueueSQL(sqlStats, update + 3, "update");
		voltExecuteSQL();
		return;
	}

	public VoltTable[] run(String word)
        {
            int read = 0;
	    int write = 0;
	    int update = 0;
    	    voltQueueSQL(sqlExists, word);
    	    VoltTable []sqlExistsT = voltExecuteSQL();
	    read ++;
	    Date now = new Date();
		
	    if(sqlExistsT[0].getRowCount() > 0)
		{
			long count = sqlExistsT[0].fetchRow(0).getLong(1);
			Long timings = sqlExistsT[0].fetchRow(0).getLong(2);
			
			if((timings - now.getTime())/ (1000 * 60 * 60) > 2)
			{
				count = 1;
			}
			else
			{
				count++;
			}
			
			voltQueueSQL(sqlUpdate, count, now.getTime(), word);
			update++;
			countStat(read, write, update);
    	     		return voltExecuteSQL();			
		}
		else
		{	
			
			
			voltQueueSQL(sqlInsert, word, 1, now.getTime());
			write++;
			countStat(read, write, update);			
    	     		return voltExecuteSQL();
		}
	}
};

