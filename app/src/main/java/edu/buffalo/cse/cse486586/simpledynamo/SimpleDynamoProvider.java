package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {
	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	static final int SERVER_PORT = 10000;
	static final String REMOTE_PORT[] = {"5554", "5556", "5558", "5560", "5562"};
	static final int TOTAL_AVDS = 5;
	static final int TOTAL_REPLICA_NODES = 2;
	static final String RING[] = new String[TOTAL_AVDS];
	private static final String KEY_FIELD = "key";
	private static final String VALUE_FIELD = "value";
	private static final String LOCAL_DHT = "@";
	private static final String ALL_DHT = "*";
	static final int fwdInsertRequestToBothNextNodes = 1;
	static final int fwdInsertRequestToCoAndRep = 2;
	static final int fwdInsertRequestToAll = 3;
	static final int fwdQueryToBothNextNodes = 4;
	static final int fwdQueryToCoAndRep = 5;
	static final int fwdQueryToAll = 6;
	static final int fwdGetAllQueryToAll = 7;
	static final int fwdDeleteToBothNextNodes = 8;
	static final int fwdDeleteToCoAndRep = 9;
	static final int fwdDeleteToAll = 10;
	private final Uri mUri;
	static Nodes prevNode;
	static Nodes prev2prevNode;
	static Nodes nextNode;
	static Nodes next2nextNode;
	static String myPort;
	static String myPortgenHash;
	static int myPortInt;
	ArrayList<Nodes> ringList;
	public SimpleDynamoProvider() {
		mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
	}

	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	public int performFileDelete(String query) {
		int i;
		File file = getContext().getFileStreamPath(query);
		if (file != null && file.exists()) {
			getContext().deleteFile(query);
			return 0;
		}
		return 1;
	}
	@Override
	public int delete(Uri uri, String query, String[] selectionArgs) {
		if (query.equals(ALL_DHT)) {
			Log.e(TAG, "NOT SUPPORTED");
			return 1;
		} else if (query.equals(LOCAL_DHT)) {
			String files[] = getContext().fileList();
			for (String temp : files) {
				getContext().deleteFile(temp);
			}
		} else {
			try {
				String queryHash = genHash(query);

				int err;
				int i;
				for(i = 0; i < TOTAL_AVDS; i++) {
					if(queryHash.compareTo(ringList.get(i).hash) <= 0) {
						if(ringList.get(i).node.equals(myPort)) {
							//TODO: Write insert code
							err = performFileDelete(query);
							if (err != 0) {
								Log.e(TAG, "DELETE FAILED DUE TO FILE NOT FOUND IN ARRAYLIST");
							}
							forwardDelete(Integer.toString(fwdDeleteToBothNextNodes), query);

						} else if (ringList.get(i).node.equals(prev2prevNode.node)) {
							//TODO: Write insert code
							err = performFileDelete(query);
							if (err != 0) {
								Log.e(TAG, "DELETE FAILED DUE TO FILE NOT FOUND IN ARRAYLIST");
							}
							forwardDelete(Integer.toString(fwdDeleteToCoAndRep), Integer.toString(i),
									Integer.toString(0), query);

						} else if (ringList.get(i).node.equals(prevNode.node)) {
							//TODO: Write insert code
							err = performFileDelete(query);
							if (err != 0) {
								Log.e(TAG, "DELETE FAILED DUE TO FILE NOT FOUND IN ARRAYLIST");
							}
							forwardDelete(Integer.toString(fwdDeleteToCoAndRep), Integer.toString(i),
									Integer.toString(1), query);

						} else {
							forwardDelete(Integer.toString(fwdDeleteToAll), Integer.toString(i), query);
						}
						break;
					}
				}
				if (i == TOTAL_AVDS) {
					if(ringList.get(0).node.equals(myPort)) {
						//TODO: Write insert code
						err = performFileDelete(query);
						if (err != 0) {
							Log.e(TAG, "DELETE FAILED DUE TO FILE NOT FOUND IN ARRAYLIST");
						}
						forwardDelete(Integer.toString(fwdDeleteToBothNextNodes), query);

					} else if (ringList.get(0).node.equals(prev2prevNode.node)) {
						//TODO: Write insert code
						err = performFileDelete(query);
						if (err != 0) {
							Log.e(TAG, "DELETE FAILED DUE TO FILE NOT FOUND IN ARRAYLIST");
						}
						forwardDelete(Integer.toString(fwdDeleteToCoAndRep), Integer.toString(0),
								Integer.toString(0), query);

					} else if (ringList.get(0).node.equals(prevNode.node)) {
						//TODO: Write insert code
						err = performFileDelete(query);
						if (err != 0) {
							Log.e(TAG, "DELETE FAILED DUE TO FILE NOT FOUND IN ARRAYLIST");
						}
						forwardDelete(Integer.toString(fwdDeleteToCoAndRep),
								Integer.toString(0), Integer.toString(1), query);

					} else {
						forwardDelete(Integer.toString(fwdDeleteToAll),
								Integer.toString(0), query);
					}

				}


			} catch (NoSuchAlgorithmException e) {

			}


		}
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean onCreate() {
		int i, j;
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portStr)));
		try {
			myPortgenHash = genHash(myPort);
		} catch (NoSuchAlgorithmException e) {
			Log.e(TAG, "Exception : " + e.toString());
		}
		myPortInt =  Integer.parseInt(portStr);
		ringList = new ArrayList<Nodes>();
		for(i = 0; i < TOTAL_AVDS; i++) {
			try {
				String hashAti = genHash(REMOTE_PORT[i]);
				Nodes nodeAti = new Nodes(REMOTE_PORT[i], hashAti);
				boolean inserted = false;
				if(ringList.isEmpty()) {
					ringList.add(nodeAti);
				} else {
					for(j = 0; j < ringList.size(); j++) {
						String hashAtj = ringList.get(j).hash;
						if(hashAti.compareTo(hashAtj) < 0) {
							ringList.add(j, nodeAti);
							inserted = true;
							break;
						}
					}
					if(!inserted) {
						ringList.add(nodeAti);
					}
				}
			} catch (NoSuchAlgorithmException e) {
				Log.e(TAG, "Exception : " + e.toString());
			}
		}
		for (i = 0; i < TOTAL_AVDS; i++) {
			if (i == 3) {
				ringList.get(i).nextnodes[0] = ringList.get(4).node;
				ringList.get(i).nextnodes[1] = ringList.get(0).node;
			} else if (i == 4) {
				ringList.get(i).nextnodes[0] = ringList.get(0).node;
				ringList.get(i).nextnodes[1] = ringList.get(1).node;
			} else {
				ringList.get(i).nextnodes[0] = ringList.get(i+1).node;
				ringList.get(i).nextnodes[1] = ringList.get(i+2).node;
			}
			if(ringList.get(i).node.equals(myPort)) {
				if(i == 0) {
					prevNode = ringList.get(4);
					prev2prevNode = ringList.get(3);
				} else if (i == 1) {
					prev2prevNode = ringList.get(4);
					prevNode = ringList.get(i-1);
				} else {
					prev2prevNode = ringList.get(i-2);
					prevNode = ringList.get(i-1);
				}
				if(i == 4) {
					nextNode = ringList.get(0);
				} else {
					nextNode = ringList.get(i+1);
				}
				if (i == 3) {
					next2nextNode = ringList.get(0);
				} else if (i == 4) {
					next2nextNode = ringList.get(1);
				} else {
					next2nextNode = ringList.get(i+2);
				}
				Log.d(TAG, "I am " + myPort);
				Log.d(TAG, "Prev2Prev Node " + prev2prevNode.node);
				Log.d(TAG, "Prev node " + prevNode.node);
				Log.d(TAG, "Next node " + nextNode.node);
				Log.d(TAG, "Next to next node " + next2nextNode.node);
			}
		}

		try {
			new RecoveryTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			Log.e(TAG, "Can't create a ServerSocket");
			return false;
		}
		// TODO Auto-generated method stub
		return true;
	}

	private class RecoveryTask extends AsyncTask<String, Void, Void> {
		protected Void doInBackground(String... params) {
			String files[] = getContext().fileList();
			int i;
			FileInputStream inputStream;
			HashMap<String, ValueVersion> keyvalMap = new HashMap<String, ValueVersion>();
			for (String temp : files) {
				if(!temp.equals("instant-run")) {
					try {
						inputStream = getContext().openFileInput(temp);
						if(inputStream != null) {
							BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
							String splitter[] = br.readLine().split(":#:");
							ValueVersion valueVersion = new ValueVersion(splitter[0],
									Integer.parseInt(splitter[1]));
							keyvalMap.put(temp, valueVersion);
							inputStream.close();
						}

					} catch (FileNotFoundException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}

				}
			}

			String queryresult[] = forwardQuery(Integer.toString(fwdGetAllQueryToAll), LOCAL_DHT);
			FileOutputStream outputStream;
			try {
				if(queryresult != null) {
					for (String server_result : queryresult) {
						if(server_result != null) {
							if(!server_result.isEmpty()) {
								String keyvals[] = server_result.split(":%:");
								for (String currentkeyval : keyvals) {
									if(currentkeyval != null) {
										if(!currentkeyval.isEmpty()) {
											String splitter[] = currentkeyval.split(":#:");
											String keygenHash = genHash(splitter[0]);
											Nodes coordinator = null;
											for(i = 0; i < TOTAL_AVDS; i++) {
												if(keygenHash.compareTo(ringList.get(i).hash) <= 0) {
													coordinator = ringList.get(i);
													break;
												}
											}
											if (i == TOTAL_AVDS) {
												coordinator = ringList.get(0);
											}
											if (coordinator.node.equals(myPort) || coordinator.equals(prevNode)
													|| coordinator.equals(prev2prevNode)) {
												ValueVersion valv = new ValueVersion(splitter[1],
														Integer.parseInt(splitter[2]));
												if(keyvalMap.containsKey(splitter[0])) {
													if (keyvalMap.get(splitter[0]).version < valv.version) {
														keyvalMap.put(splitter[0], valv);
														outputStream = getContext().openFileOutput(splitter[0], Context.MODE_PRIVATE);
														String val1 = splitter[1] + ":#:" + splitter[2];
														outputStream.write(val1.getBytes());
														outputStream.close();
													}
												} else {
													keyvalMap.put(splitter[0], valv);
													outputStream = getContext().openFileOutput(splitter[0], Context.MODE_PRIVATE);
													String val1 = splitter[1] + ":#:" + splitter[2];
													outputStream.write(val1.getBytes());
													outputStream.close();
												}
											}
										}
									}
								}
							}
						}
					}
				}
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}

			return null;
		}
	}

	public int performFileInsert(ContentValues values) {

		String key = values.getAsString(KEY_FIELD);
		String val = values.getAsString(VALUE_FIELD);
		String val1;
		int newversion;
		String previousval[];
		FileInputStream inputStream;
		FileOutputStream outputStream;
		File file = getContext().getFileStreamPath(key);
		if (file != null && file.exists()) {
			try {
				inputStream = getContext().openFileInput(key);
				if (inputStream != null) {
					BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
					previousval = br.readLine().split(":#:");
					inputStream.close();
					outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
					newversion = Integer.parseInt(previousval[1]) + 1;
					val1 = val + ":#:" + Integer.toString(newversion);
					outputStream.write(val1.getBytes());
					outputStream.close();
				} else {
					Log.e(TAG, "InputStream returned NULL");
					return 1;
				}
			} catch (FileNotFoundException e) {
				Log.e(TAG, e.toString());
				return 1;
			} catch (IOException e) {
				Log.e(TAG, e.toString());
				return 1;
			}
		} else {
			try {
				val1 = val + ":#:1";
				outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
				outputStream.write(val1.getBytes());
				outputStream.close();
			} catch (IOException es) {
				Log.e(TAG, es.toString());
				return 1;
			} catch (NullPointerException es) {
				Log.e(TAG, es.toString());
				return 1;
			}
		}
		return 0;
	}

	public Uri serverInsert(Uri uri, ContentValues values) {
		int err;
		err = performFileInsert(values);
		if(err != 0) {
			Log.e(TAG, "SERVER INSERT FAILED DUE TO FAILURE IN FILE INSERT");
		}
		return uri;
	}

	public void serverDelete(String query) {
		int err;
		err = performFileDelete(query);
		if(err != 0) {
			Log.e(TAG, "SERVER DELETE FAILED DUE TO FILE NOT FOUND IN ARRAYLIST");
		}
	}
	@Override
	public Uri insert(Uri uri, ContentValues values) {
		String key = values.getAsString(KEY_FIELD);
		String keygenHash = null;
		int i;
		try{
			keygenHash = genHash(key);
		} catch (NoSuchAlgorithmException e){
			Log.e(TAG, "Exception : " + e.toString());
		}
		String val = values.getAsString(VALUE_FIELD);

		int err;
		for(i = 0; i < TOTAL_AVDS; i++) {
			if(keygenHash.compareTo(ringList.get(i).hash) <= 0) {
				if(ringList.get(i).node.equals(myPort)) {
					//TODO: Write insert code
					err = performFileInsert(values);
					if (err != 0) {
						Log.e(TAG, "INSERT FAILED DUE TO FAILURE IN FILE INSERT");
					}
					err = forwardInsert(Integer.toString(fwdInsertRequestToBothNextNodes), key, val);

				} else if (ringList.get(i).node.equals(prev2prevNode.node)) {
					//TODO: Write insert code
					err = performFileInsert(values);
					if (err != 0) {
						Log.e(TAG, "INSERT FAILED DUE TO FAILURE IN FILE INSERT");
					}
					err = forwardInsert(Integer.toString(fwdInsertRequestToCoAndRep),
							Integer.toString(i), Integer.toString(0), key, val);

				} else if (ringList.get(i).node.equals(prevNode.node)) {
					//TODO: Write insert code
					err = performFileInsert(values);
					if (err != 0) {
						Log.e(TAG, "INSERT FAILED DUE TO FAILURE IN FILE INSERT");
					}
					err = forwardInsert(Integer.toString(fwdInsertRequestToCoAndRep),
							Integer.toString(i), Integer.toString(1), key, val);

				} else {

					err = forwardInsert(Integer.toString(fwdInsertRequestToAll),
							Integer.toString(i), key, val);

				}
				break;
			}
		}
		if (i == TOTAL_AVDS) {
			if(ringList.get(0).node.equals(myPort)) {
				//TODO: Write insert code
				err = performFileInsert(values);
				if (err != 0) {
					Log.e(TAG, "INSERT FAILED DUE TO FAILURE IN FILE INSERT");
				}
				err = forwardInsert(Integer.toString(fwdInsertRequestToBothNextNodes), key, val);

			} else if (ringList.get(0).node.equals(prev2prevNode.node)) {
				//TODO: Write insert code
				err = performFileInsert(values);
				if (err != 0) {
					Log.e(TAG, "INSERT FAILED DUE TO FAILURE IN FILE INSERT");
				}
				err = forwardInsert(Integer.toString(fwdInsertRequestToCoAndRep),
						Integer.toString(0), Integer.toString(0), key, val);

			} else if (ringList.get(0).node.equals(prevNode.node)) {
				//TODO: Write insert code
				err = performFileInsert(values);
				if (err != 0) {
					Log.e(TAG, "INSERT FAILED DUE TO FAILURE IN FILE INSERT");
				}
				err = forwardInsert(Integer.toString(fwdInsertRequestToCoAndRep),
						Integer.toString(0), Integer.toString(1), key, val);

			} else {
				err = forwardInsert(Integer.toString(fwdInsertRequestToAll),
						Integer.toString(0), key, val);

			}

		}
		// TODO Auto-generated method stub
		return uri;
	}

	public String performFileQuery(String query) {
		FileInputStream inputStream;
		String qresult = "";
		if(query.equals(ALL_DHT)) {
			return qresult;
		} else if (query.equals(LOCAL_DHT)) {
			try {
				String files[] = getContext().fileList();
				for (String temp : files) {
					inputStream = getContext().openFileInput(temp);
					if(inputStream != null) {
						BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
						if (qresult.isEmpty()) {
							qresult = temp + ":#:" + br.readLine();
						} else {
							qresult = qresult + ":%:" + temp + ":#:" + br.readLine();
						}
						inputStream.close();
					}
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
				return qresult;
			} catch (IOException e) {
				e.printStackTrace();
				return qresult;
			}
			return qresult;
		} else {
			File file = getContext().getFileStreamPath(query);
			if (file != null && file.exists()) {
				try {
					inputStream = getContext().openFileInput(query);
					if (inputStream != null) {
						BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
						qresult = br.readLine();
						inputStream.close();
					}
					return qresult;
				} catch (FileNotFoundException e) {
					e.printStackTrace();
					return qresult;
				} catch (IOException e) {
					e.printStackTrace();
					return qresult;
				}
			} else {
				return qresult;
			}
		}
	}
	public String serverQuery(String query) {
		String qresult;
		qresult = performFileQuery(query);
		if(qresult.isEmpty()) {
			Log.e(TAG, "SERVER QUERY FAILED DUE TO FAILURE IN FILE QUERY");
		}
		return qresult;
	}
	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		String query = selection;
		int i;
		if(query.equals(ALL_DHT)) {
			HashMap<String, ValueVersion> keyvalMap = new HashMap<String, ValueVersion>();
			MatrixCursor matrixCursor = new MatrixCursor(new String[] {KEY_FIELD, VALUE_FIELD});
			String localresult = performFileQuery(LOCAL_DHT);
			if(!localresult.isEmpty()) {
				String keyvals[] = localresult.split(":%:");
				for (String currentkeyval : keyvals) {
					if(!currentkeyval.isEmpty()) {
						String splitter[] = currentkeyval.split(":#:");
						ValueVersion valv = new ValueVersion(splitter[1],
								Integer.parseInt(splitter[2]));
						if(keyvalMap.containsKey(splitter[0])) {
							if (keyvalMap.get(splitter[0]).version < valv.version) {
								keyvalMap.put(splitter[0], valv);
							}
						} else {
							keyvalMap.put(splitter[0], valv);
						}
					}
				}
			}
			String queryresult[] = forwardQuery(Integer.toString(fwdGetAllQueryToAll), LOCAL_DHT);
			if(queryresult != null) {
				for (String server_result : queryresult) {
					if(server_result != null) {
						if(!server_result.isEmpty()) {
							String keyvals[] = server_result.split(":%:");
							for (String currentkeyval : keyvals) {
								if(currentkeyval != null) {
									if(!currentkeyval.isEmpty()) {
										String splitter[] = currentkeyval.split(":#:");
										ValueVersion valv = new ValueVersion(splitter[1],
												Integer.parseInt(splitter[2]));
										if(keyvalMap.containsKey(splitter[0])) {
											if (keyvalMap.get(splitter[0]).version < valv.version) {
												keyvalMap.put(splitter[0], valv);
											}
										} else {
											keyvalMap.put(splitter[0], valv);
										}
									}
								}
							}
						}
					}
				}
			}

			for (String key : keyvalMap.keySet()) {
				String value = keyvalMap.get(key).value;
				matrixCursor.addRow(new Object[] {key, value});
			}
			return matrixCursor;
		} else if(query.equals(LOCAL_DHT)) {
			String result = "";
			String splitter[];
			MatrixCursor matrixCursor = new MatrixCursor(new String[] {KEY_FIELD, VALUE_FIELD});
			FileInputStream inputStream;
			try {
				String files[] = getContext().fileList();
				for(String temp : files) {
					inputStream = getContext().openFileInput(temp);
					if (inputStream != null) {
						BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
						result = br.readLine();
						splitter = result.split(":#:");
						matrixCursor.addRow(new Object[] {temp, splitter[0]});
						inputStream.close();
					}
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			return matrixCursor;
		} else {
			try {
				String querygenHash = genHash(query);
				String queryval[] = new String[3];
				String finalval = "";
				String splitter[];
				int highest = 0;
				for(i = 0; i < TOTAL_AVDS; i++) {
					if (querygenHash.compareTo(ringList.get(i).hash) <= 0) {
						if(ringList.get(i).node.equals(myPort)) {
							//TODO: Write Query Code
							queryval[0] = performFileQuery(query);
							String queryresult[] = forwardQuery(Integer.toString(fwdQueryToBothNextNodes), query);
							queryval[1] = queryresult[0];
							queryval[2] = queryresult[1];
						} else if (ringList.get(i).node.equals(prev2prevNode.node)) {
							//TODO: Write Query Code
							queryval[0] = performFileQuery(query);
							String queryresult[] = forwardQuery(Integer.toString(fwdQueryToCoAndRep),
									Integer.toString(i), Integer.toString(0), query);
							queryval[1] = queryresult[0];
							queryval[2] = queryresult[1];
						} else if (ringList.get(i).node.equals(prevNode.node)) {
							//TODO: Write Query Code
							queryval[0] = performFileQuery(query);
							String queryresult[] = forwardQuery(Integer.toString(fwdQueryToCoAndRep),
									Integer.toString(i), Integer.toString(1), query);
							queryval[1] = queryresult[0];
							queryval[2] = queryresult[1];
						} else {
							String queryresult[] = forwardQuery(Integer.toString(fwdQueryToAll),
									Integer.toString(i), query);
							queryval[0] = queryresult[0];
							queryval[1] = queryresult[1];
							queryval[2] = queryresult[2];
						}
						break;
					}
				}
				if (i == TOTAL_AVDS) {
					if(ringList.get(0).node.equals(myPort)) {
						//TODO: Write Query Code
						queryval[0] = performFileQuery(query);
						String queryresult[] = forwardQuery(Integer.toString(fwdQueryToBothNextNodes), query);
						queryval[1] = queryresult[0];
						queryval[2] = queryresult[1];
					} else if (ringList.get(0).node.equals(prev2prevNode.node)) {
						//TODO: Write Query Code
						queryval[0] = performFileQuery(query);
						String queryresult[] = forwardQuery(Integer.toString(fwdQueryToCoAndRep),
								Integer.toString(0), Integer.toString(0), query);
						queryval[1] = queryresult[0];
						queryval[2] = queryresult[1];
					} else if (ringList.get(0).node.equals(prevNode.node)) {
						//TODO: Write Query Code
						queryval[0] = performFileQuery(query);
						String queryresult[] = forwardQuery(Integer.toString(fwdQueryToCoAndRep),
								Integer.toString(0), Integer.toString(1), query);
						queryval[1] = queryresult[0];
						queryval[2] = queryresult[1];
					} else {
						String queryresult[] = forwardQuery(Integer.toString(fwdQueryToAll),
								Integer.toString(0), query);
						queryval[0] = queryresult[0];
						queryval[1] = queryresult[1];
						queryval[2] = queryresult[2];
					}
				}
				boolean first = true;
				for (i = 0; i < 3; i++) {
					if (queryval[i] != null) {
						if(!queryval[i].isEmpty()) {
							splitter = queryval[i].split(":#:");
							int currentversion = Integer.parseInt(splitter[1]);
							if(first) {
								highest = currentversion;
								finalval = splitter[0];
								first = false;
							}
							if(currentversion > highest) {
								highest = currentversion;
								finalval = splitter[0];

							}
						}
					}
				}
				MatrixCursor matrixCursor = new MatrixCursor(new String[] {KEY_FIELD, VALUE_FIELD});
				if(!finalval.isEmpty()) {
					matrixCursor.addRow(new Object[] {query, finalval});
				}
				return matrixCursor;
			} catch (NoSuchAlgorithmException e) {
				Log.e(TAG, "Exception : "+ e.toString());
			}

		}
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	private class ServerTask extends AsyncTask<ServerSocket, Integer, Void> {
		protected Void doInBackground(ServerSocket... sockets){
			ServerSocket serverSocket =sockets[0];
			while (true) {
				try {
					Socket server = serverSocket.accept();
					DataInputStream in = new DataInputStream(server.getInputStream());
					DataOutputStream out = new DataOutputStream(server.getOutputStream());
					String recvdMsg[] = in.readUTF().split(":#:");
					String queryresult;
					switch (Integer.parseInt(recvdMsg[0])) {
						case fwdInsertRequestToBothNextNodes:
						case fwdInsertRequestToCoAndRep:
						case fwdInsertRequestToAll:
							ContentValues mContentValues = new ContentValues();
							mContentValues.put(KEY_FIELD, recvdMsg[1]);
							mContentValues.put(VALUE_FIELD, recvdMsg[2]);
							serverInsert(mUri, mContentValues);
							out.writeUTF("OK");
							out.flush();
							out.close();
							in.close();
							server.close();
							break;
						case fwdQueryToBothNextNodes:
						case fwdQueryToCoAndRep:
						case fwdQueryToAll:
						case fwdGetAllQueryToAll:
							queryresult = serverQuery(recvdMsg[1]);
							out.writeUTF(queryresult);
							out.flush();
							String recvd = in.readUTF();
							if(!recvd.equals("OK")) {
								Log.e(TAG, "Received " + recvd + "instead of OK");
							}
							out.close();
							in.close();
							server.close();
							break;
						case fwdDeleteToBothNextNodes:
						case fwdDeleteToCoAndRep:
						case fwdDeleteToAll:
							serverDelete(recvdMsg[1]);
							out.writeUTF("OK");
							out.flush();
							out.close();
							in.close();
							server.close();
							break;
					}
				} catch (IOException e) {
					Log.e(TAG, "Exception : " + e.toString());
				}
			}
		}
		protected void onProgressUpdate(Integer... neighbors){

		}
	}

	public String[] forwardQuery(String... args) {
		int i;
		String fwdnodes[] = new String[3];
		String allnodes[] = new String[4];
		String queryresult[];
		switch (Integer.parseInt(args[0])) {

			case fwdQueryToBothNextNodes:
				 queryresult = new String[2];
				fwdnodes[0] = nextNode.node;
				fwdnodes[1] = next2nextNode.node;
				for (i = 0; i < 2; i++) {
					try {
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(fwdnodes[i]) * 2);
						DataInputStream in = new DataInputStream(socket.getInputStream());
						DataOutputStream out = new DataOutputStream(socket.getOutputStream());
						out.writeUTF(fwdQueryToBothNextNodes + ":#:" + args[1]);
						out.flush();
						queryresult[i] = in.readUTF();
						out.writeUTF("OK");
						out.close();
						in.close();
						socket.close();
					} catch (EOFException e) {
						Log.e(TAG, "Exception : " + e.toString());
					} catch (UnknownHostException e) {
						Log.e(TAG, "Exception : " + e.toString());
					} catch (IOException e) {
						Log.e(TAG, "Exception : " + e.toString());
					}
				}
			break;

			case fwdQueryToCoAndRep:
				queryresult = new String[2];
				fwdnodes[0] = ringList.get(Integer.parseInt(args[1])).node;
				if(args[2].equals("0")) {
					fwdnodes[1] = ringList.get(Integer.parseInt(args[1])).nextnodes[0];
				} else if(args[2].equals("1")) {
					fwdnodes[1] = ringList.get(Integer.parseInt(args[1])).nextnodes[1];
				}

				for (i = 0; i < 2; i++) {
					try {
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(fwdnodes[i]) * 2);
						DataInputStream in = new DataInputStream(socket.getInputStream());
						DataOutputStream out = new DataOutputStream(socket.getOutputStream());
						out.writeUTF(fwdQueryToCoAndRep + ":#:" + args[3]);
						out.flush();
						queryresult[i] = in.readUTF();
						out.writeUTF("OK");
						out.close();
						in.close();
						socket.close();
					} catch (EOFException e) {
						Log.e(TAG, "Exception : " + e.toString());
					}  catch (UnknownHostException e) {
						Log.e(TAG, "Exception : " + e.toString());
					} catch (IOException e) {
						Log.e(TAG, "Exception : " + e.toString());
					}
				}
				break;

			case fwdQueryToAll:
				queryresult = new String[3];
				fwdnodes[0] = ringList.get(Integer.parseInt(args[1])).node;
				fwdnodes[1] = ringList.get(Integer.parseInt(args[1])).nextnodes[0];
				fwdnodes[2] = ringList.get(Integer.parseInt(args[1])).nextnodes[1];
				for (i = 0; i < 3; i++) {
					try {
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(fwdnodes[i]) * 2);
						DataInputStream in = new DataInputStream(socket.getInputStream());
						DataOutputStream out = new DataOutputStream(socket.getOutputStream());
						out.writeUTF(fwdQueryToAll + ":#:" + args[2]);
						out.flush();
						queryresult[i] = in.readUTF();
						out.writeUTF("OK");
						out.close();
						in.close();
						socket.close();
					} catch (EOFException e) {
						Log.e(TAG, "Exception : " + e.toString());
					}  catch (UnknownHostException e) {
						Log.e(TAG, "Exception : " + e.toString());
					} catch (IOException e) {
						Log.e(TAG, "Exception : " + e.toString());
					}
				}
				break;

			case fwdGetAllQueryToAll:
				queryresult = new String[4];
				int j = 0;
				for (i = 0; i < 5; i++) {
					if(!ringList.get(i).node.equals(myPort)){
						try {
							Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									Integer.parseInt(ringList.get(i).node) * 2);
							DataInputStream in = new DataInputStream(socket.getInputStream());
							DataOutputStream out = new DataOutputStream(socket.getOutputStream());
							out.writeUTF(fwdGetAllQueryToAll + ":#:" + args[1]);
							out.flush();
							queryresult[j] = in.readUTF();
							out.writeUTF("OK");
							out.close();
							in.close();
							socket.close();
							j++;
						} catch (EOFException e) {
							Log.e(TAG, "Exception : " + e.toString());
						}  catch (UnknownHostException e) {
							Log.e(TAG, "Exception : " + e.toString());
						} catch (IOException e) {
							Log.e(TAG, "Exception : " + e.toString());
						}					}
				}
				break;
			default:
				queryresult = null;
				Log.e(TAG, "This log should never be printed");
				break;
		}
		return queryresult;
	}

	public int forwardInsert(String... args) {
		int i;
		String fwdnodes[] = new String[3];
		switch (Integer.parseInt(args[0])) {

			case fwdInsertRequestToBothNextNodes:
				fwdnodes[0] = nextNode.node;
				fwdnodes[1] = next2nextNode.node;
				for (i = 0; i < 2; i++) {
					try {
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(fwdnodes[i]) * 2);

						DataInputStream in = new DataInputStream(socket.getInputStream());
						DataOutputStream out = new DataOutputStream(socket.getOutputStream());
						out.writeUTF(fwdInsertRequestToBothNextNodes + ":#:" + args[1] + ":#:" + args[2]);
						out.flush();
						String recvd = in.readUTF();
						if(!recvd.equals("OK")) {
							Log.e(TAG, "Received " + recvd + "instead of OK");
						}
						out.close();
						in.close();
						socket.close();
					} catch (EOFException e) {
						Log.e(TAG, "Exception : " + e.toString());
					}  catch (UnknownHostException e) {
						Log.e(TAG, "Exception : " + e.toString());
					} catch (IOException e) {
						Log.e(TAG, "Exception : " + e.toString());
					}
				}
			break;

			case fwdInsertRequestToCoAndRep:
				fwdnodes[0] = ringList.get(Integer.parseInt(args[1])).node;
				if(args[2].equals("0")) {
					fwdnodes[1] = ringList.get(Integer.parseInt(args[1])).nextnodes[0];
				} else if(args[2].equals("1")) {
					fwdnodes[1] = ringList.get(Integer.parseInt(args[1])).nextnodes[1];
				}

				for (i = 0; i < 2; i++) {
					try {
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(fwdnodes[i]) * 2);
						DataInputStream in = new DataInputStream(socket.getInputStream());
						DataOutputStream out = new DataOutputStream(socket.getOutputStream());
						out.writeUTF(fwdInsertRequestToCoAndRep + ":#:" + args[3] + ":#:" + args[4]);
						out.flush();
						String recvd = in.readUTF();
						if(!recvd.equals("OK")) {
							Log.e(TAG, "Received " + recvd + "instead of OK");
						}
						out.close();
						in.close();
						socket.close();
					} catch (EOFException e) {
						Log.e(TAG, "Exception : " + e.toString());
					}  catch (UnknownHostException e) {
						Log.e(TAG, "Exception : " + e.toString());
					} catch (IOException e) {
						Log.e(TAG, "Exception : " + e.toString());
					}
				}
			break;

			case fwdInsertRequestToAll:
				fwdnodes[0] = ringList.get(Integer.parseInt(args[1])).node;
				fwdnodes[1] = ringList.get(Integer.parseInt(args[1])).nextnodes[0];
				fwdnodes[2] = ringList.get(Integer.parseInt(args[1])).nextnodes[1];
				for (i = 0; i < 3; i++) {
					try {
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(fwdnodes[i]) * 2);
						DataInputStream in = new DataInputStream(socket.getInputStream());
						DataOutputStream out = new DataOutputStream(socket.getOutputStream());
						out.writeUTF(fwdInsertRequestToCoAndRep + ":#:" + args[2] + ":#:" + args[3]);
						out.flush();
						String recvd = in.readUTF();
						if(!recvd.equals("OK")) {
							Log.e(TAG, "Received " + recvd + "instead of OK");
						}
						out.close();
						in.close();
						socket.close();
					} catch (EOFException e) {
						Log.e(TAG, "Exception : " + e.toString());
					}  catch (UnknownHostException e) {
						Log.e(TAG, "Exception : " + e.toString());
					} catch (IOException e) {
						Log.e(TAG, "Exception : " + e.toString());
					}
				}
			break;

			default:
				return 1;
		}
	return 0;
	}

	public void forwardDelete(String... args) {
		int i;
		String fwdnodes[] = new String[3];
		switch (Integer.parseInt(args[0])) {

			case fwdDeleteToBothNextNodes:
				fwdnodes[0] = nextNode.node;
				fwdnodes[1] = next2nextNode.node;
				for (i = 0; i < 2; i++) {
					try {
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(fwdnodes[i]) * 2);

						DataInputStream in = new DataInputStream(socket.getInputStream());
						DataOutputStream out = new DataOutputStream(socket.getOutputStream());
						out.writeUTF(fwdDeleteToBothNextNodes + ":#:" + args[1]);
						out.flush();
						String recvd = in.readUTF();
						if(!recvd.equals("OK")) {
							Log.e(TAG, "Received " + recvd + "instead of OK");
						}
						out.close();
						in.close();
						socket.close();
					} catch (EOFException e) {
						Log.e(TAG, "Exception : " + e.toString());
					}  catch (UnknownHostException e) {
						Log.e(TAG, "Exception : " + e.toString());
					} catch (IOException e) {
						Log.e(TAG, "Exception : " + e.toString());
					}
				}
				break;

			case fwdDeleteToCoAndRep:
				fwdnodes[0] = ringList.get(Integer.parseInt(args[1])).node;
				if(args[2].equals("0")) {
					fwdnodes[1] = ringList.get(Integer.parseInt(args[1])).nextnodes[0];
				} else if(args[2].equals("1")) {
					fwdnodes[1] = ringList.get(Integer.parseInt(args[1])).nextnodes[1];
				}

				for (i = 0; i < 2; i++) {
					try {
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(fwdnodes[i]) * 2);
						DataInputStream in = new DataInputStream(socket.getInputStream());
						DataOutputStream out = new DataOutputStream(socket.getOutputStream());
						out.writeUTF(fwdDeleteToCoAndRep + ":#:" + args[3]);
						out.flush();
						String recvd = in.readUTF();
						if(!recvd.equals("OK")) {
							Log.e(TAG, "Received " + recvd + "instead of OK");
						}
						out.close();
						in.close();
						socket.close();
					} catch (EOFException e) {
						Log.e(TAG, "Exception : " + e.toString());
					}  catch (UnknownHostException e) {
						Log.e(TAG, "Exception : " + e.toString());
					} catch (IOException e) {
						Log.e(TAG, "Exception : " + e.toString());
					}
				}
				break;

			case fwdDeleteToAll:
				fwdnodes[0] = ringList.get(Integer.parseInt(args[1])).node;
				fwdnodes[1] = ringList.get(Integer.parseInt(args[1])).nextnodes[0];
				fwdnodes[2] = ringList.get(Integer.parseInt(args[1])).nextnodes[1];
				for (i = 0; i < 3; i++) {
					try {
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(fwdnodes[i]) * 2);
						DataInputStream in = new DataInputStream(socket.getInputStream());
						DataOutputStream out = new DataOutputStream(socket.getOutputStream());
						out.writeUTF(fwdDeleteToAll + ":#:" + args[2]);
						out.flush();
						String recvd = in.readUTF();
						if(!recvd.equals("OK")) {
							Log.e(TAG, "Received " + recvd + "instead of OK");
						}
						out.close();
						in.close();
						socket.close();
					} catch (EOFException e) {
						Log.e(TAG, "Exception : " + e.toString());
					} catch (UnknownHostException e) {
						Log.e(TAG, "Exception : " + e.toString());
					} catch (IOException e) {
						Log.e(TAG, "Exception : " + e.toString());
					}
				}
				break;

			default:
				Log.e(TAG, "THIS LOG SHOULD NEVER BE PRINTED");
				break;
		}
	}

	private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}

	private class Nodes {
		String node;
		String nextnodes[];
		String hash;
		Nodes(String node, String hash){
			this.node = node;
			this.hash = hash;
			this.nextnodes = new String[2];
		}
	}

	private class ValueVersion {
		String value;
		int version;
		ValueVersion(String value, int version) {
			this.value = value;
			this.version = version;
		}
	}
}
