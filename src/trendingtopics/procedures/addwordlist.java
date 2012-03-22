package trendingtopics.procedures;
import org.voltdb.*;
import java.util.Date;

@ProcInfo(
	singlePartition= false 

)


public class addwordlist extends VoltProcedure {

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
	


	public VoltTable[] countStat(int read, int write, int update)
	{	

		voltQueueSQL(sqlStats, read+3, "read");
		voltQueueSQL(sqlStats, write, "write");
		voltQueueSQL(sqlStats, update + 3, "update");
		return voltExecuteSQL();
		
	}

	public VoltTable[] run(String [] wordList, int size)
        {
            int read = 0;
	    int write = 0;
	    int update = 0;

	    for (int i = 0; i < size; i ++) 
	    {
		String word = wordList[i];
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
				voltExecuteSQL();
	    	     					
			}
			else
			{	
			
				voltQueueSQL(sqlInsert, word, 1, now.getTime());
				write++;
				voltExecuteSQL();
							
	    	     		
			}
	     }

		return countStat(read, write, update);
		 
	}
};

