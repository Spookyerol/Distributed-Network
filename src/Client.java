import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import java.util.Scanner;

public class Client {
	
	public Client() {}
	
	public static void main(String args[]) {
		try {
			// Since client is useless without a front end to connect to we make one
			FEServer.main(args);
			// Connect to the server address
		    Registry registry = LocateRegistry.getRegistry("127.0.0.1", 10000);
		    FEInterface stub = (FEInterface) registry.lookup("FE");

		    Scanner scan = new Scanner(System.in);
		    while(true) {
		    	System.out.println("'QUERY' for, or 'SUBMIT' rating?");
		    	System.out.println("type 'EXIT' to terminate program");
		    	String inp = scan.next();
		    	if(inp.toLowerCase().equals("query")) {
		    		System.out.println("Provide userID and movieID in format 'userID,movieID'");
		    		inp = scan.next();
		    		String[] input = inp.split(",");
		    		String resp = stub.getRating(Integer.valueOf(input[0]), Integer.valueOf(input[1]));
		    		System.out.println(resp);
		    	}
		    	else if(inp.toLowerCase().equals("submit")) {
		    		System.out.println("Provide userID, movieID and your score in format 'userID,movieID,score'");
		    		inp = scan.next();
		    		String[] input = inp.split(",");
		    		String resp = stub.submitRating(Integer.valueOf(input[0]), Integer.valueOf(input[1]), input[2]);
		    		System.out.println(resp);
		    	}
		    	else if(inp.toLowerCase().equals("exit")) {
		    		System.err.println("Shutting down...");
		    		// Shutdown the the client and frontend server
		    		stub.terminate(registry);
		    		break;
		    	}
		    	else {
		    		System.err.println("Error:Command not recognized, please type 'QUERY' or 'SUBMIT'");
		    		continue;
		    	}
		    }
		    scan.close();
		} catch (Exception e) {
			System.err.println("Client exception: " + e.toString());
			e.printStackTrace();
		}
	}
}
