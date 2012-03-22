package trendingtopics;
import org.voltdb.*;
import org.voltdb.client.*;
import java.lang.String;
import java.io.*; 

public class trendingtopics {

	public static void main(String[] args) throws Exception {
		/*
		* Instantiate a client and connect to the database.
		*/
		org.voltdb.client.Client myApp;
		myApp = ClientFactory.createClient();
		myApp.createConnection("localhost");


		 FileInputStream fstream = new FileInputStream("testfile.txt");
		  // Get the object of DataInputStream
		  DataInputStream in = new DataInputStream(fstream);
		  BufferedReader br = new BufferedReader(new InputStreamReader(in));
		  String strLine;
		  //Read File Line By Line
		  while ((strLine = br.readLine()) != null)   {
			  String [] words = strLine.split(" ");

			//for(int i = 0; i < words.length; i++)
			myApp.callProcedure("addwordlist", words, words.length);
	
		  }
		  //Close the input stream
		  in.close();
		  
		

		
		/*
		* Retrieve the message.
		*/
		final ClientResponse response = myApp.callProcedure("gettopics", "test");
		if (response.getStatus() != ClientResponse.SUCCESS){
			System.err.println(response.getStatusString());
			System.exit(-1);
		}

		VoltTable resultTable = response.getResults()[0];
		while(resultTable.advanceRow())
		{

			System.out.printf("%s %d\n", resultTable.getString(0), resultTable.getLong(1));
	
		}
	}
};


