import org.apache.xmlrpc.*;
import java.util.*;
import java.util.Vector;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.nio.charset.StandardCharsets;

public class Server {
	//need a fileinfo hash map --MetadataStore
	// need a block hash map --BlockStore
	protected Hashtable<String,Vector> metadata_store_hash = new Hashtable<String, Vector>();
	protected Hashtable<String,byte[]> block_store_hash = new Hashtable<String, byte[]>();

	public Vector getfileinfo(String filename){
		return this.metadata_store_hash.get(filename);
	}
	// A simple ping, simply returns True
	public boolean ping() {
		System.out.println("Ping()");
		return true;
	}

	// Given a hash value, return the associated block
	public byte[] getblock(String hashvalue) {
		System.out.println("GetBlock(" + hashvalue + ")");

		/*
		byte[] blockData = new byte[16];
		for (int i = 0; i < blockData.length; i++) {
			blockData[i] = (byte) i;
		}
		*/
		System.out.println("Current hash being stored in server is "+this.block_store_hash.keySet());
		return this.block_store_hash.get(hashvalue);
	}

	// Store the provided block
	//The hash value of b would get computed by server again. You dont need to pass h.
	//Please dont change the signature of the functions.

	public boolean putblock(byte[] blockData) {
		System.out.println("PutBlock()");
		try{
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			String hash_string = Base64.getEncoder().encodeToString(digest.digest(blockData)); //encode to string representation 
			this.block_store_hash.put(hash_string, blockData);
			System.out.println("PUTBLOCK : Remote index "+this.block_store_hash);
			return true;
		} catch (NoSuchAlgorithmException e){
			System.err.println("SERVER : in putblock " + e);
		}
		return false; 
	}

	// Determine which of the provided blocks are on this server
	public Vector hasblocks(Vector hashlist) {
		System.out.println("HasBlocks()");
		Vector result = new Vector();
		Iterator iter = hashlist.iterator();
		while(iter.hasNext()){
			Object nextObject = iter.next();
			if(this.block_store_hash.containsKey(nextObject)){
				result.add(nextObject);
			}
		}
		return result;
	}

	// Returns the server's FileInfoMap
	public Hashtable getfileinfomap() {
		System.out.println("GetFileInfoMap()");

		Hashtable result = new Hashtable(metadata_store_hash);

		return result;
	}

	//returns version number
	// Update's the given entry in the fileinfomap
	public Vector updatefile(String filename, int version, Vector hashlist) {
		//hash list belongs to the file in CLIENT
		System.out.println("UpdateFile(" + filename + ")");
		Vector result = new Vector();

		//if the file exists in the cloud
		if(this.metadata_store_hash.containsKey(filename)){ //if file does exists on RI
			Integer cloud_version = (Integer) this.metadata_store_hash.get(filename).get(0); //version is type Integer
			if(version == cloud_version){ //if version is the same 
				Vector<String> cloud_hashlist = (Vector<String>) this.metadata_store_hash.get(filename).get(1);
				if(!cloud_hashlist.equals(hashlist)){ //if file hashlist is changed/updated in local file
					Vector metadata = new Vector();
					Integer new_version = new Integer(version+1); //increment version in cloud 
					metadata.add(new_version);
					metadata.add(hashlist);
					this.metadata_store_hash.put(filename, metadata);
					result.add(version+1);//return version + 1
					result.add(new Integer(1));//1 for SUCESSFUL update
					//SUCESSFUL update has 1 at index 1 and version at index 0 [version, 1]
				}
				else{ //if hashlist stays the same 
					result.add(version);
					result.add(new Integer(0)); //unsucessful update because file stays the same 
				}
			}
			else {
				result.add(cloud_version);
				result.add(new Integer(0)); //0 for UNSUCESSFUL update
			}

		}
		else{ //if file doesnt exist on RI
			Vector metadata = new Vector();
			metadata.add(new Integer(version)); //should be 1 because it's the first version
			metadata.add(hashlist);

			//stores on fileinfo map
			this.metadata_store_hash.put(filename,metadata);

			result.add(new Integer(version)); //should be 1
			result.add(new Integer(1)); //indicates sucessfull insertion.
		}
		return result;
	}

	public static void main (String [] args) {

		try {

			System.out.println("Attempting to start XML-RPC Server...");

			WebServer server = new WebServer(8080);
			//WebServer server = new WebServer(9000);
			server.addHandler("surfstore", new Server()); //creates new instance of server
			server.start();

			System.out.println("Started successfully.");
			System.out.println("Accepting requests. (Halt program to stop.)");

		} catch (Exception exception){
			System.err.println("Server: " + exception);
		}
	}
}