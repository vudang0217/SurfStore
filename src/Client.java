import java.util.*;
import org.apache.xmlrpc.*;

import java.nio.charset.StandardCharsets;
//import java.nio.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.io.File; //for file processing
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.util.Vector; 
import java.io.FileWriter; //to write to file

public class Client {

//CALCULATE HASH VAL
	public static String[] compute_SHA(File file, int block_size, MessageDigest digest){
		try{
			byte[] file_byte_arr = Files.readAllBytes(file.toPath()); //StandardCharsets.UTF_8
	
			int file_length = file_byte_arr.length;
			System.out.println("Hashing "+ file.getName());
			System.out.println("SIZE OF "+file.getName()+" = "+file_length);
			Vector<byte[]> file_blocks = new Vector(); //THE VECTOR/LIST Containing the hash value 
			int num_chunks = file_length/block_size; //number of chunks for that file depending on blocksize 
			
			for(int i = 0; i<num_chunks; i++){
				byte[] curr_block = Arrays.copyOfRange(file_byte_arr, i*block_size, (i*block_size)+block_size); //slice the byte array into blocks
				byte[] curr_hash = digest.digest(curr_block); //perform the hashing 
				file_blocks.add(curr_hash);
			}

			int last_block_size = file_length % block_size;

			if(last_block_size !=0){
				byte[] last_block = Arrays.copyOfRange(file_byte_arr, file_length-last_block_size, file_length); //slice the last byte arr
				byte[] last_hash =  digest.digest(last_block);
				file_blocks.add(last_hash);
				num_chunks++;
			}
			System.out.println(file.getName()+" divided into "+num_chunks+" blocks");
			String[] result = new String[num_chunks];
			for(int i = 0; i<num_chunks; i++){
				String hash_string = Base64.getEncoder().encodeToString(file_blocks.get(i)); //encode to string representation 
				result[i] = hash_string;
			}
			return result;
		} catch(IOException e) {
			System.err.println(e);
			System.err.println("EXCEPTION THROWN in compute_SHA when reading file.");
			System.exit(1);
		}
		return null;
	}

//DOWNLOAD (HAS DELETION IN IT)
	public static void download_from_cloud(XmlRpcClient client,String basedir_name,String current_file,Vector<String> hashlist){
		//param hashlist is from the remote index (updated one)
		try{
			//GET THE FILEINFO MAP
			//byte[] data_buffer = {};
			System.out.println("DOWNLOAD : "+current_file);	
			File file = new File(basedir_name, current_file);
			if(file.exists()){
				file.delete();
			}

			//DELETION -- if file is deleted on cloud 
			if(hashlist.size()==1 && hashlist.get(0).equals("0")){
				System.out.println(current_file + "has been deleted");
				return;
			}
			file.createNewFile();

			//if created within try block, automatically close it on exit	
			//fos writes byte[] to file
			FileOutputStream fos = new FileOutputStream(file);

			Iterator iter = hashlist.iterator(); //HASHLIST of SERVER
			Vector params;
			while(iter.hasNext()){
				params = new Vector();
				params.add(iter.next());
				Object file_data = client.execute("surfstore.getblock", params);
				System.out.println(file_data);
				fos.write((byte[])file_data); //check if this overwrites or continue to write????
			}
			//System.out.println("getblocl() successful");
		} catch (Exception e){
			System.err.println("IN DOWNLOAD FROM CLOUD -- Client: " + e);
		} 
	}

//UPLOAD 
	public static void upload_to_cloud(XmlRpcClient client,Vector<String> hashlist, String basedir_name,String current_file, int block_size){
		
		try{
			Vector params = new Vector();
			//CHANGES BLOCKSTORE : upload blocks to the cloud hash table 
			File file = new File(basedir_name, current_file);
			if(!file.exists()){
				System.out.println("UPLOAD : "+current_file+" based dir has been moved to tombstone record on cloud, doing nothing.");
				return; 
			}
			byte[] file_byte_arr = Files.readAllBytes(file.toPath());
			int num_chunks = hashlist.size();
			params.add(hashlist);
			Vector<String> existed_blocks = (Vector<String>)client.execute("surfstore.hasblocks", params);

			for(int i = 0; i < num_chunks; i++){
				String hash_string = hashlist.get(i);
				if(!existed_blocks.contains(hash_string)){ //if block doesnt exist in the cloud
					params = new Vector();
					int end_index = (i == num_chunks-1)?file_byte_arr.length:(i+1)*block_size; //retrieve last block size 
					System.out.println("UPLOAD : end_index is :"+end_index);
					params.add(Arrays.copyOfRange(file_byte_arr,i*block_size,end_index));
					client.execute("surfstore.putblock", params);
				}
			}
			//now what?

		} catch(Exception e) {
			System.err.println("In UPLOAD_TO_CLOUD -- Client" + e );
		}

	}

//MAIN
   public static void main (String [] args) {
   
	  if (args.length != 3) {
	  	System.err.println("Usage: Client host:port /basedir blockSize");
		System.exit(1);
	  }

      try {
		 //XmlRpcClient client = new XmlRpcClient("http://localhost:8080/RPC2"); //USE THIS TO TEST LOCAL SERVER
		 //do we need to append rpc2
		 //SAMPLE CODE 
		 /*
		 XmlRpcClient client = new XmlRpcClient(args[0]);
         Vector params = new Vector();

		 // Test ping
		 params = new Vector();
		 client.execute("surfstore.ping", params);
		 System.out.println("Ping() successful");

		 // Test PutBlock
		 params = new Vector();
		 byte[] blockData = new byte[10];
		 params.addElement(blockData);
         boolean putresult = (boolean) client.execute("surfstore.putblock", params);
		 System.out.println("PutBlock() successful");

		 // Test GetBlock
		 params = new Vector();
		 params.addElement("h0");
         byte[] blockData2 = (byte[]) client.execute("surfstore.getblock", params);
		 System.out.println("GetBlock() successfully read in " + blockData2.length + " bytes");
		*/
		/*-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+--+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+*/
		/*-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+--+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+*/
		/*-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+ CODER STARTS HERE -+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+*/
		/*-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+--+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+*/
		/*-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+--+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+*/

		String basedir_name = Paths.get(args[1]).normalize().toAbsolutePath().toString();
		System.out.println("PATH Base directory : " +basedir_name);

		File basedir_file = new File(basedir_name);
		File[] basedir_listing = basedir_file.listFiles(); //gather all files in base dir and put in an array
		//boolean has_index = false; //check if index exist

		//size of each block to be hashed
		int block_size = Integer.parseInt(args[2]);

		//HASHMAP BASEDIR : to store after going through files in the dir and computing SHA256
		Hashtable<String, Vector> hash_base_dir = new Hashtable<String, Vector>();

		//HASHMAP LOCALINDEX : to store hashlist retrieved from local index 
		Hashtable<String, Vector> hash_local_index = new Hashtable<String, Vector>();	


		//if the directory exists is not empty 
		//computer SHA value and put in hash table
		if ((basedir_listing != null)&&(basedir_listing.length >0)){
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			for(File basedir_child : basedir_listing){
				//skip the index.txt file
				if(basedir_child.getName().equals("index.txt")){
					continue;
				}
				//implement the has SHA thing 
				//https://stackoverflow.com/questions/5531455/how-to-hash-some-string-with-sha256-in-java
				//convert file to byte array
				//divide into blocks 
				//hash each block 
				/*
				The client should first scan the base directory, and for each file, compute that files hash 
				list. The client should then consult the local index file and compare the results, to see whether
				(1) there are now new files in the base directory that arent in the index file, or (2) files that
				are in the index file, but have changed since the last time the client was executed (i.e., the hash
				list is different).
				*/

				// calling helper method computing SHA VALUE 
				String[] hash_SHA_arr = compute_SHA(basedir_child, block_size, digest);
				Vector<String> hash_SHA_vect = new Vector<String>(Arrays.asList(hash_SHA_arr));
				hash_base_dir.put(basedir_child.getName(),hash_SHA_vect);

				//put file name in set 
				//set_baseDir.add(basedir_child.getName());

				/*
				System.out.println("String hash value is : ");
				for(String s : hash_SHA_arr){
					System.out.println(s);
				}
				*/
			}
		}
		else{
			System.out.println(basedir_name + " is not a directory or is EMPTY (empty basedir).");
		}
		File index_file = new File(basedir_name, "index.txt");
		//if there is no index.txt, create new file in the system 
		if(index_file.createNewFile()){
			System.out.println("CREATED index.txt!");
		} 
		else{
			System.out.println("FOUND Existing index.txt");
		}
		
		//loop through the localindex file and create a hashtable
		Scanner scanner =  new Scanner(index_file);
		while(scanner.hasNextLine()){
			String[] scanner_index_line = scanner.nextLine().split("," , 0);
			//set_localIndex.add(scanner_index_line[0]);//put file names in set

			//value on hash table
			// filename = [4, [h1,h2,h3,h4]] (version number and string vector )
			Vector scanner_ver_string = new Vector(); 

			Integer scanner_version = new Integer(Integer.parseInt(scanner_index_line[1]));
			scanner_ver_string.add(scanner_version);
			
			String[] scanner_hash_list = scanner_index_line[2].split(" ",0);
			Vector<String> scanner_hash_string_vect = new Vector<String>(Arrays.asList(scanner_hash_list));
			scanner_ver_string.add(scanner_hash_string_vect);

			//hash the file into local index hash -> "name" : <interger, string[]>
			hash_local_index.put(scanner_index_line[0],scanner_ver_string); 
		}
		System.out.println("local index hash table is "+hash_local_index);
		System.out.println("basedir hash table is "+hash_base_dir);
		//Connect the client to a server
		System.out.println("Connecting to a server : " + args[0]);
		//String[] address_to_connect = args[0].split(":",0);
		XmlRpcClient client = new XmlRpcClient("http://"+args[0]+"/RPC2");
		System.out.println("Connected.");

		//GET THE FILEINFO MAP
        Vector params = new Vector();
		Hashtable<String,Vector> hash_remote_index = (Hashtable<String,Vector>)client.execute("surfstore.getfileinfomap", params);
		//System.out.println("getfileinfomap() successful");

		System.out.println("REMOTE INDEX BEFORE UPDATE : "+hash_remote_index);
		// AS SETS : 
		// BD = base directory 
		// RI = remote index
		// LI = local index
		// for case 1, find union of BD and LI and find set difference between that and RI  

		//sets contain file names of Remote Index (from getinfomap), Local Index, and Base directory 
		Set<String> set_RI = hash_remote_index.keySet();
		Set<String> set_LI = hash_local_index.keySet();
		Set<String> set_BD = hash_base_dir.keySet();

		//set operations 
		//RI Intersect LI
		Set<String> in_RI_in_LI = new HashSet<String>(set_RI);
		in_RI_in_LI.retainAll(set_LI);

		//RI - LI set difference
		Set<String> in_RI_notin_LI = new HashSet<String>(set_RI);
		in_RI_notin_LI.removeAll(set_LI);

		//BD - RI set difference
		Set<String> notin_RI_in_BD = new HashSet<String>(set_BD);	
		notin_RI_in_BD.removeAll(set_RI);

		//for the case when remote idex contains files that local directory does not (RI - LI)
		System.out.println("MAIN : DOING {RI} - {LI}");
		for(String current_file : in_RI_notin_LI) {
			Vector ri_version_hashlist = hash_remote_index.get(current_file); //-->[ver,[h1,h2,h3]]
			//Integer ver = ri_version_hashlist.get(0); we don't need version?
			Vector<String> hashlist = (Vector<String>)ri_version_hashlist.get(1);

			//helper method download block from cloud and CREATE file
			download_from_cloud(client,basedir_name,current_file,hashlist);		
		}

		//for the case when remote index does not have files that are in local directories 
		// BD - RI 
		// need to updatefile()
		System.out.println("MAIN : DOING {BD} - {RI}");
		for(String current_file : notin_RI_in_BD){
			Vector<String> hashlist = hash_base_dir.get(current_file);
			upload_to_cloud(client,hashlist, basedir_name, current_file,block_size);//version 1 because first one created
			Vector params2 = new Vector();
			params2.add(current_file);
			params2.add(new Integer(1));
			params2.add(hashlist);
			//update meta data with new files 
			//public Vector updatefile(String filename, int version, Vector hashlist)
			client.execute("surfstore.updatefile", params2);
			
		}
		//Vector params = new Vector();
		System.out.println("MAIN : DOING {RI} INTERSECT {LI}");
		for(String current_file : in_RI_in_LI){
			//dont need to store local version because the updatefile method will update it 
			System.out.println("MAIN : I am working on "+current_file);
			System.out.println("MAIN : metadata in local index is "+hash_local_index.get(current_file));
			Integer current_file_version = (Integer) hash_local_index.get(current_file).get(0);
			File file = new File(basedir_name, current_file); //current file on basedir
			Vector<String> current_file_hashlist;
			if(file.exists()){
				current_file_hashlist = (Vector<String>) hash_base_dir.get(current_file); //basedir meta data is vector size of 1, there is no version 
			}
			else{ //if file is recently deleted in basedir 
				current_file_hashlist = new Vector<String>();
				current_file_hashlist.add("0");
			}
			params = new Vector();
			params.add(current_file);
			params.add(current_file_version); //local index version 
			params.add(current_file_hashlist); //local index hashlist
			//CHANGES METADATA STORE : update getinfomap, doesnt matter for now-changes the 
			//public Vector updatefile(String filename, int version, Vector hashlist)
			Vector<Integer> upload_status = (Vector<Integer>)client.execute("surfstore.updatefile", params);
			System.out.println("MAIN : I am getting from updatefile "+upload_status);
			int returned_version = upload_status.get(0);
			int returned_code = upload_status.get(1);
			if(returned_code==1){ //if the cloud infomap gets updated with the local file 
				upload_to_cloud(client, current_file_hashlist, basedir_name, current_file, block_size);
			}
			else if(returned_version > current_file_version){ //if the local index has outdated version and needs to be updated. 
				//update local file. 
				download_from_cloud(client, basedir_name, current_file, 
						(Vector<String>) hash_remote_index.get(current_file).get(1));
			}

		}
		System.out.println("MAIN : Writing to LOCAL INDEX (index.txt)");
		params = new Vector();
		index_file.delete();
		index_file = new File(basedir_name, "index.txt");
		index_file.createNewFile();
		hash_remote_index = (Hashtable)client.execute("surfstore.getfileinfomap", params);
		//WRITE TO LOCAL FILE
		FileWriter fw = new FileWriter(index_file);
		Set<String> keys_RI = hash_remote_index.keySet();
		for(String keys : keys_RI){ //each iteration = each line
			System.out.println("MAIN : metadata of "+ keys+ " is " +hash_remote_index.get(keys));
			fw.write(keys+","+hash_remote_index.get(keys).get(0)+",");
			Vector<String> hashlist = (Vector<String>) hash_remote_index.get(keys).get(1);
			for(int i=0; i<hashlist.size(); i++){
				fw.write(hashlist.get(i));
				if(i!=hashlist.size()-1){
					fw.write(" ");
				}
			}
			fw.write("\n");
		}
		fw.flush();
		fw.close();
		scanner.close();
      } catch (Exception exception) {
         System.err.println("Client: " + exception);
      }
   }
}