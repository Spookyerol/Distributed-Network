
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import java.util.Date;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class RServer implements RInterface {
	
	private HashMap<Integer, ArrayList<String>> movieMap; //movieID, (title, genres)
	private HashMap<List<Integer>, List<String>> ratingsMap; //(userID, movieID), (rating, time stamp)
	private int[] vectorStamp; //each cell points to a replicant that is in the network by index, the number in the cell represents the number of updates that this server thinks the corresponding replicants have
	private int id; //this index points to the cell in vectorStamp that corresponds to this object/server
	private String[] bindingList;
	
	public RServer(int networkSize) {
		movieMap = new HashMap<Integer, ArrayList<String>>();
		ratingsMap = new HashMap<List<Integer>, List<String>>();
		vectorStamp = new int[networkSize];
		readFile("movies.csv", "movie");
		readFile("ratings.csv", "ratings");
	}
	
	public static void main(String args[]) {
		
		try {
			int networkSize = Integer.valueOf(args[0]);
			String[] bindings = new String[networkSize];
			RServer[] servers = new RServer[networkSize];
			
		    // Get registry
		    Registry registry = LocateRegistry.getRegistry("127.0.0.1", 10000);
			for(int i = 0;i < networkSize;i++) {
				
			    // Create server object
			    RServer obj = new RServer(networkSize);
			    servers[i] = obj;
			    
			    // Create remote object stub from server object
			    RInterface stub = (RInterface) UnicastRemoteObject.exportObject(obj, 0);
	
			    // Bind the remote object's stub in the registry
			    String binding = "R" + Integer.toString(i+1);
			    registry.bind(binding, stub);
			    bindings[i] = binding;
			    obj.setId(i);
			    
			    System.err.println("Replicant " + binding + " ready at port " + Integer.toString(10000));
			}
			
			for(int i = 0;i < bindings.length;i++) {
				servers[i].setAvailBindings(bindings);
			}
		} catch (Exception e) {
		    System.err.println("Replicant exception: " + e.toString());
		    e.printStackTrace();
		}
		
		Runnable r = new Runnable() {
	         public void run() {
	        	 try {
	        		 while(true) { // keep looping through in the background
		        		 Registry registry = LocateRegistry.getRegistry("127.0.0.1", 10000);
		        		 String[] servers = registry.list();
		        		 int noServers = servers.length;
		        		 for(int i=0; i < registry.list().length;i++) {
		        			 if(servers[i].equals("FE")) {
		        				 noServers--;
		        			 }
		        		 }
		        		 Status[] states = new Status[noServers];
		        		 for(int i=0;i < noServers;i++) {
		        			 String binding = "R" + Integer.toString(i+1);
		        			 RInterface replicant = (RInterface) registry.lookup(binding);
		        			 states[i] = replicant.getStatus();
		        			 
		        		 }
		        		 for(int i=0;i < noServers;i++) {
		        			 if(states[i] == Status.OK) {
		        				 String binding = "R" + Integer.toString(i+1);
		        				 RInterface replicant = (RInterface) registry.lookup(binding);
		        				 replicant.gossipForUpdates(registry, states);
		        			 }
		        		 }
	        			 System.out.println("Successfully gossiped.");
	        			 Thread.sleep(10000); // wait for arg / 1000 seconds
	        		 }
	        	 } catch (Exception e) {
	     		    System.err.println("Gossip exception: " + e.toString());
	    		    e.printStackTrace();
	    		}
	         }
	    };

	    new Thread(r).start(); //this line is async - will not wait for the task in runnable to finish
	}
	
	public void readFile(String fileName, String type) {
		String path = "../ml-latest-small/" + fileName;
		Path pathToFile = Paths.get(path);
		
		try (BufferedReader br = Files.newBufferedReader(pathToFile, StandardCharsets.UTF_8)) {
			
			String line = br.readLine();
			line = br.readLine(); //first line is just the column names so we skip
			while (line != null) {
				if(type.equals("movie")) {
					addMovieEntry(line);
				}
				else if(type.equals("ratings")) {
					addRatingsEntry(line);
				}
				
				line = br.readLine();
			}
			
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}
	
	public void addMovieEntry(String line) {
		int i = line.indexOf(",");
		int j = line.lastIndexOf(",");
		String[] attr = {line.substring(0, i), line.substring(i+1, j), line.substring(j+1)};
		ArrayList<String> data = new ArrayList<>();
		data.add(attr[1]);
		data.add(attr[2]);
		this.movieMap.put(Integer.valueOf(attr[0]), data);
	}
	
	public void addRatingsEntry(String line) {
		String[] attr = line.split(",");
		int userID = Integer.valueOf(attr[0]);
		int movieID = Integer.valueOf(attr[1]);
		this.ratingsMap.put(Collections.unmodifiableList(Arrays.asList(userID, movieID)), Collections.unmodifiableList(Arrays.asList(attr[2], attr[3])));
	}
	
	public boolean checkForUpdates(int counter, int otherId) {
		if(this.vectorStamp[otherId] < counter) { //this means that the other replicant has more updates than this server expected, this trusts that the last used replica is up to date with all other replicas - since it served the previous request
		//note that this replicant could still have updates that the other one does not posess yet
			return true; //need updates
		}
		else {
			return false; //no updates found
		}
	}
	
	public void gossipForUpdates(Registry registry, Status[] states) {
		try {
			for(int i=0;i < bindingList.length;i++) {
				RInterface replicant = (RInterface) registry.lookup(bindingList[i]);
				if(!replicant.equals(this)) {
					if(states[replicant.getId()] == Status.OK) {
						if(checkForUpdates(this.vectorStamp[replicant.getId()], replicant.getId())) {
							this.applyUpdates(replicant);
							this.vectorStamp[replicant.getId()] = replicant.getVectorStamp()[replicant.getId()];
						}
					}
					// We cannot gossip with a replicant that is not available
				}
			}
		} catch (Exception e) {
		    System.err.println("Replicant exception: " + e.toString());
		    e.printStackTrace();
		}
	}
	
	public void getAvailableUpdates(String[] checkVector) {
		try {
			int updatesToGet = Integer.valueOf(checkVector[1]);
			String updateReplica = checkVector[0];
			Registry registry = LocateRegistry.getRegistry("127.0.0.1", 10000);
			boolean update = false;
			if(!checkVector[0].equals("FE")) {
				int otherId = Integer.valueOf(updateReplica.substring(1)) - 1; //retrieves the id of the other replicant by separating out the 'R' from the numerical part
				update = checkForUpdates(updatesToGet, otherId);
			}
			else {
				String[] servers = registry.list();
				int noServers = servers.length - 1;
	       		for(int i=0;i < noServers;i++) {
	       			String binding = "R" + Integer.toString(i+1);
	       			RInterface replicant = (RInterface) registry.lookup(binding);
	       			if(!replicant.equals(this)) {
		       			while(replicant.getStatus() == Status.OFFLINE) { //since updating like this is high priority we still go through even if the server is overloaded
							Thread.sleep(100); //We have to wait until the server is back up again
						}
	       				updatesToGet = replicant.getVectorStamp()[replicant.getId()];
	       				if(checkForUpdates(updatesToGet, i)) {
		       				applyUpdates(replicant);
		       				this.vectorStamp[replicant.getId()] = updatesToGet;
	       				}
	       			}
	       		}
			}
			if(update) {
				RInterface replicant = (RInterface) registry.lookup(updateReplica);
				while(replicant.getStatus() == Status.OFFLINE) { //since updating like this is high priority we still go through even if the server is overloaded
					Thread.sleep(100); //We have to wait until the server is back up again
				}
				applyUpdates(replicant);
				this.vectorStamp[replicant.getId()] = Integer.valueOf(updatesToGet);
				
			}
		} catch (Exception e) {
		    System.err.println("Replicant exception: " + e.toString());
		    e.printStackTrace();
		}
	}
	
	public void applyUpdates(RInterface replicant) {
		try {
			HashMap<List<Integer>, List<String>> otherMap = replicant.getRatings();
			for (Map.Entry<List<Integer>, List<String>> entry : otherMap.entrySet()) {
				if(this.ratingsMap.get(entry.getKey()) == null) { //there is an additional entry that must be added
					this.ratingsMap.put(entry.getKey(), entry.getValue());
					this.vectorStamp[id] = vectorStamp[id] + 1;
				}
				else if(this.ratingsMap.get(entry.getKey()) != entry.getValue()) { //a data point has been modified
					List<String> valuesSelf = this.ratingsMap.get(entry.getKey());
					long timeStampSelf = Long.parseLong(valuesSelf.get(1));
					//check to see which copy of data was most recently changed
					if(timeStampSelf < Long.parseLong(entry.getValue().get(1))) { //the other replicant has more recent data so we update
						this.ratingsMap.put(entry.getKey(), entry.getValue());
						this.vectorStamp[id] = vectorStamp[id] + 1;
					}
					//else we keep our copy of the data
				}
			}
		} catch (Exception e) {
		    System.err.println("Replicant exception: " + e.toString());
		    e.printStackTrace();
		}
	}
	
	public String[] getRating(int userID, int movieID, String[] checkVector) {
		try {
			getAvailableUpdates(checkVector);
			ArrayList<String> movieData = movieMap.get(movieID);
			if(movieData == null) {
				String msg = "Error:Specified movie does not exist.";
				String[] response = {msg, Integer.toString(vectorStamp[id]), bindingList[id]};
				return response;
			}
			else {
				List<String> rating = ratingsMap.get(Arrays.asList(userID, movieID));
				if(rating == null) {
					String msg = "Error:This rating does not exist.";
					String[] response = {msg, Integer.toString(vectorStamp[id]), bindingList[id]};
					return response;
				}
				else {
					String msg = "Rating by " + Integer.toString(userID) + " for " + movieData.get(0) + " is " + rating.get(0) + " at time " + rating.get(1);
					String[] response = {msg, Integer.toString(vectorStamp[id]), bindingList[id]};
					return response;
				}
			}
		} catch (Exception e) {
		    System.err.println("Replicant exception: " + e.toString());
		    e.printStackTrace();
		}
		return null;
	}
	
	public String[] submitRating(int userID, int movieID, String score, String[] checkVector) {
		try {
			//getAvailableUpdates(checkVector);
			ArrayList<String> movieData = movieMap.get(movieID);
			if(movieData == null) {
				String msg = "Error:Specified movie does not exist.";
				String[] response = {msg, Integer.toString(vectorStamp[id]), bindingList[id]};
				return response;
			}
			else {
				boolean update = true;
				List<String> rating = ratingsMap.get(Arrays.asList(userID, movieID));
				if(rating == null) {
					update = false;
				}
				ArrayList<String> data = new ArrayList<String>();
				data.add(score);
				Date date = new Date();
				long timeStamp = date.getTime();
				String time = String.valueOf(timeStamp);
				data.add(time);
				ratingsMap.put(Collections.unmodifiableList(Arrays.asList(userID, movieID)), data);
				String msg;
				if(!update) {
					msg = "New rating by " + Integer.toString(userID) + " submitted for " + movieData.get(0) + " with score " + score + " at time " + time;
				}
				else {
					msg = "Existing rating for " + movieData.get(0) + " by " + Integer.toString(userID) + " updated to score " + score + " at time " + time;
				}
				this.vectorStamp[id] = vectorStamp[id] + 1;
				String[] response = {msg, Integer.toString(vectorStamp[id]), bindingList[id]};
				return response;
			}
		} catch (Exception e) {
			System.err.println("Replicant exception: " + e.toString());
		    e.printStackTrace();
		}
		return null;
	}
	
	public Status getStatus() {
		Status status = Status.OK;
		double p = Math.random();
		if(p < 0.05) {
			status = Status.OFFLINE;
		}
		else if(p < 0.3) {
			status = Status.OVERLOADED;
		}
		return status;
	}
	
	public HashMap<List<Integer>, List<String>> getRatings() {
		return this.ratingsMap;
	}
	
	public int getId() {
		return this.id;
	}
	
	public void setId(int id) {
		this.id = id;
	}
	
	public void setAvailBindings(String[] bindings) {
		this.bindingList = bindings;
	}
	
	public int[] getVectorStamp() {
		return this.vectorStamp;
	}
}



//rmiregistry <port number> on cmd before attempting to run.