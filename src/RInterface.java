import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;

import java.util.HashMap;
import java.util.List;

public interface RInterface extends Remote {
	String[] getRating(int userID, int movieID, String[] checkVector) throws RemoteException;
	
	String[] submitRating(int userID, int movieID, String score, String[] checkVector) throws RemoteException;
	
	Status getStatus() throws RemoteException;
	
	HashMap<List<Integer>, List<String>> getRatings() throws RemoteException;
	
	int getId() throws RemoteException;
	
	int[] getVectorStamp() throws RemoteException;
	
	void gossipForUpdates(Registry registry, Status[] states) throws RemoteException;
}
