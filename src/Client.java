import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import java.util.Scanner;

public class Client {
	
	public Client() {}
	
	public static void main(String[] args) {
		String host = (args.length < 1) ? null : args[0];
		try {

		    // Get registry
		    Registry registry = LocateRegistry.getRegistry("127.0.0.1", 3000);

		    // Create a stub
		    FEInterface stub = (FEInterface) registry.lookup("FE");

		    // Invoke a remote method
		    //String response = stub.testRemote();
		    //System.out.println("response: " + response);
		    Scanner scan = new Scanner(System.in);
		    while(true) {
		    	System.out.println("'QUERY' for, or 'SUBMIT' rating?");
		    	System.out.println("type 'EXIT' to terminate program");
		    	String inp = scan.next();
		    	if(inp.toLowerCase().equals("query")) {
		    		System.out.println("Provide userID and movieID in format 'userID,movieID'");
		    		inp = scan.next();
		    		//Ideally some error handling on input
		    		String[] input = inp.split(",");
		    		String resp = stub.getRating(Integer.valueOf(input[0]), Integer.valueOf(input[1]));
		    		System.out.println(resp);
		    	}
		    	else if(inp.toLowerCase().equals("submit")) {
		    		System.out.println("Provide userID, movieID and your score in format 'userID,movieID,score'");
		    		inp = scan.next();
		    		//Ideally some error handling on input
		    		String[] input = inp.split(",");
		    		String resp = stub.submitRating(Integer.valueOf(input[0]), Integer.valueOf(input[1]), input[2]);
		    		System.out.println(resp);
		    	}
		    	else if(inp.toLowerCase().equals("exit")) {
		    		System.err.println("Shutting down...");
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
