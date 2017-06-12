package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
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
import android.database.MergeCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;

import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import static android.content.ContentValues.TAG;



public class SimpleDynamoProvider extends ContentProvider {

	// Contacts Table Columns names
	private static final String KEY = "key";
	private static final String VALUE = "value";
	private static final String DEVICEID = "deviceid";
    private static final String VERSION = "version";
	private static final String TABLENAME = "MyTable";


	private static final String TESTKEY = "testkey";
	private static final String TESTVALUE ="testvalue";
	private static final String TESTTABLE = "TestTable";

	private static final String INSERT = "Insert";

	private static final String SINGLEQUERY = "SingleQuery";
	private static final String ATQUERY = "@Query";

	private static final String SINGLEDELETE = "SingleDelete";
	private static final String ATDELETE = "@Delete";

	private static final String RECOVERY ="Recovery";

	private Databasehelper m_helper;
	static final int SERVER_PORT = 10000;


	// Port numbers of all the devices
	static final int AVD0_PORT = 11108;
	static final int AVD1_PORT = 11112;
	static final int AVD2_PORT = 11116;
	static final int AVD3_PORT = 11120;
	static final int AVD4_PORT = 11124;

	//Device Ids of all the devices
	static final int AVD0_ID = 5554;
	static final int AVD1_ID = 5556;
	static final int AVD2_ID = 5558;
	static final int AVD3_ID = 5560;
	static final int AVD4_ID = 5562;

    HashMap<Integer,String> devicemap;
	int m_selfPortNumber = 0;
	boolean m_IsRecoveryInProgress = false;
	boolean m_IsReoveryTaskCreated = false;

    Object synctoken = new Object();


	public class CircularList<E> extends ArrayList<E> {

		@Override
		public E get(int index)
		{
			if(index < 0)
				index = index + size();
			return super.get(index % size());
		}
	}

	public CircularList<Integer> m_NodeList;


     public class Databasehelper extends SQLiteOpenHelper
	 {

		public Databasehelper(Context context)
		{
			super(context, "MyDatabase", null, 2);
			Log.e("db:", "Databasehandler Constructor");
		}

		// Creating Tables
		@Override
		public void onCreate(SQLiteDatabase db) {
			Log.v("db:", "Database Create");
			String QUERY = "CREATE TABLE " + TABLENAME + "("
					+ KEY + " TEXT PRIMARY KEY,"
					+ VALUE + " TEXT,"
					+ DEVICEID +" TEXT,"
                    + VERSION +" TEXT"+")";


			db.execSQL(QUERY);

			String TESTTABLEQUERY = "CREATE TABLE " + TESTTABLE + "("
					+ TESTKEY + " TEXT PRIMARY KEY,"
					+ TESTVALUE + " TEXT" + ")";

			db.execSQL(TESTTABLEQUERY);
		}

		@Override
		public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
			// Drop older table if existed
			db.execSQL("DROP TABLE IF EXISTS " + "Table");
			// Create tables again
			onCreate(db);
		}

		public void Insert(String key,String Msg ,String deviceid)
		{
			Log.v("content insert", "enter");
            String Query = "SELECT * FROM MyTable WHERE key =" +"\""+ key+"\"";
            Cursor cursor = getReadableDatabase().rawQuery(Query, null);

            String version = "";
            if(cursor.getCount() <= 0)
            {
                version = String.valueOf(1);
            }
            else
            {


				Log.e(TAG, "else check 1");
                cursor.moveToPosition(0);
                Log.e(TAG,"cursorcount="+String.valueOf(cursor.getCount()));
                Log.e(TAG,"colcount="+String.valueOf(cursor.getColumnCount()));
                int keyIndex = cursor.getColumnIndex("key");
                int valueIndex = cursor.getColumnIndex("value");

                int valuedeviceid= cursor.getColumnIndex("deviceid");

                Log.e(TAG,"wasup:"+String.valueOf(keyIndex)+":"+String.valueOf(valueIndex)+":"+String.valueOf(valuedeviceid));


                String returnKey = cursor.getString(keyIndex);


                String returnValue = cursor.getString(valueIndex);


                String returndeviceid = cursor.getString(valuedeviceid);





                int valueversion = cursor.getColumnIndex("version");
                String returnversion = cursor.getString(valueversion);

                int v = Integer.valueOf(returnversion);
                v++;
                version = String.valueOf(v);
				Log.e(TAG, "else check 2");
            }


			Log.e(TAG, "else check 3");
			String INSERTQUERY = "INSERT OR REPLACE INTO MyTable (key,value,deviceid,version) VALUES(\"" + key + "\",\"" + Msg + "\",\"" + deviceid+"\",\"" + version+"\")";

			Log.v("content insert", INSERTQUERY);
			getWritableDatabase().execSQL(INSERTQUERY);
		}


         public void RecoveryInsert(String inputstr)
         {

             Log.v("RecoveryInsert", "enter");
             SQLiteDatabase db = getWritableDatabase();
             try {

                  db.beginTransaction();
                  if (inputstr != null && !inputstr.isEmpty())
                  {
                     String[] arr = inputstr.split("#");
                     for (int i = 0; i <= arr.length - 4; i = i + 4)
                     {


						 ContentValues cv = new ContentValues();
						 cv.put("key",arr[i]);
						 cv.put("value",arr[i+1]);
						 cv.put("deviceid",arr[i+2]);
						 cv.put("version",arr[i+3]);

                         String Query = "SELECT * FROM MyTable WHERE key =" +"\""+ arr[i]+"\"";

                         Cursor cursor = db.rawQuery(Query,null);
                         if(cursor.getCount()>0)
                         {

                             cursor.moveToPosition(0);
                             int valueversion = cursor.getColumnIndex("version");
                             String returnversion = cursor.getString(valueversion);

                             if(Integer.valueOf(arr[i+3]) > Integer.valueOf(returnversion))
                             {

                                 db.insertWithOnConflict(TABLENAME,null,cv,SQLiteDatabase.CONFLICT_REPLACE);
                             }

                         }
						 else
						 {
							 db.insert(TABLENAME,null,cv);
						 }

                     }
                 }
                 db.setTransactionSuccessful();
             }
             catch (Exception e) {
                 e.printStackTrace();
             }

             finally
             {
                db.endTransaction();
             }
         }


		public void Delete(String selection)
		{
			SQLiteQueryBuilder qbuild = new SQLiteQueryBuilder();
			qbuild.setTables(TABLENAME);
			Log.e(TAG, "delete enter");
			String Query = "";
			if(selection.compareTo("@")==0 || selection.compareTo("*")==0) {
				Query = "DELETE FROM MyTable";
			}
			else
			{
				Query = "DELETE FROM MyTable WHERE key =" +"\""+ selection+"\"";
			}
			SQLiteDatabase db =getWritableDatabase();
			db.execSQL(Query);
			db.close();
		}


		public Cursor Query(String selection)
		{
			Log.e(TAG,"main Query Enter --check1");
			SQLiteQueryBuilder qbuild = new SQLiteQueryBuilder();
			qbuild.setTables(TABLENAME);

			String Query = "";

			if(selection.compareTo("@")==0 || selection.compareTo("*")==0)
			{
				Query = "SELECT key,value FROM MyTable";
				Log.e(TAG,"main Query Enter --check2");
			}
			else
			{
				Query = "SELECT key,value,version FROM MyTable WHERE key =" +"\""+ selection+"\"";
				Log.e(TAG,"main Query Enter --check3");
			}

			Log.e(TAG,"main Query Enter --check4");
			Cursor cursor = getReadableDatabase().rawQuery(Query, null);
			Log.e(TAG ,"cursor value needed="+String.valueOf(cursor.getCount()));
			return cursor;
		}




		 public Cursor RecoveryQuery(boolean IsSingle,String deviceidvalue1,String deviceidvalue2)
		 {
			 Log.e(TAG,"RecoveryQuery Query Enter --check1");
			 SQLiteQueryBuilder qbuild = new SQLiteQueryBuilder();
			 qbuild.setTables(TABLENAME);

			 String Query = "";

			 if(IsSingle)
			 {
				 Log.e(TAG,"RecoveryQuery --check2");
				 Query = "SELECT * FROM MyTable WHERE deviceid = " + "\"" + deviceidvalue1 + "\"";
			 }
			 else
			 {
				 Log.e(TAG,"RecoveryQuery --check3");
				 Query = "SELECT * FROM MyTable WHERE deviceid =" + "\"" + deviceidvalue1 + "\"" + "OR deviceid =" + "\"" + deviceidvalue2 + "\"";
			 }


			 Cursor cursor = getReadableDatabase().rawQuery(Query, null);
			 Log.e(TAG ,"cursor value needed="+String.valueOf(cursor.getCount()));

             Log.e(TAG,"RecoveryQuery Query Exit --check2");
			 return cursor;
		 }

		public int TestTableQueryCount()
		{
			SQLiteQueryBuilder qbuild = new SQLiteQueryBuilder();
			qbuild.setTables(TESTTABLE);

			String Query = "";
			Query =  "SELECT * FROM TestTable";
			Cursor cursor = getReadableDatabase().rawQuery(Query, null);
            return cursor.getCount();

		}

		 public void TestTableInsert()
		 {
			 String Testkey = "Testkey0";
			 String TestMsg = "TestMsg0";

			 String QUERY = "INSERT OR REPLACE INTO TestTable (testkey,testvalue) VALUES(\"" + Testkey + "\",\"" + TestMsg + "\")";
			 getWritableDatabase().execSQL(QUERY);

		 }
	}


	public int IdentifyNodeForKey(String HashedKey)
	{

         if(HashedKey.compareTo(devicemap.get(AVD4_ID)) > 0 && HashedKey.compareTo(devicemap.get(AVD1_ID)) <= 0)
		    return AVD1_PORT;
		 else if(HashedKey.compareTo(devicemap.get(AVD1_ID)) > 0 && HashedKey.compareTo(devicemap.get(AVD0_ID)) <= 0)
			return AVD0_PORT;
		 else if(HashedKey.compareTo(devicemap.get(AVD0_ID)) > 0 && HashedKey.compareTo(devicemap.get(AVD2_ID)) <= 0)
			 return AVD2_PORT;
		 else if(HashedKey.compareTo(devicemap.get(AVD2_ID)) > 0 && HashedKey.compareTo(devicemap.get(AVD3_ID)) <= 0)
			 return AVD3_PORT;
		 else
		     return AVD4_PORT;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs)
	{
        synchronized (synctoken) {
            if(m_IsReoveryTaskCreated)
            {

                try {
                    wait();
                } catch (InterruptedException e) {
                    Log.e(TAG, "InterruptedException");
                }

            }
            // TODO Auto-generated method stub
            if (selection.compareTo("@") == 0) {
                Log.e(TAG, "@ Delete Done");
                m_helper.Delete("@");
            } else if (selection.compareTo("*") == 0) {
                Log.e(TAG, "Star Delete Started");
                StarDeleteHandler();
            } else {
                Log.e(TAG, "Single Delete Started");
                String HashedKey = GenerateHashWrapper(selection);
                SingleDeleteHandler(HashedKey, selection);
            }

            return 0;
        }
	}

	public void SingleDeleteHandler(String HashedKey,String key)
	{
		// TODO Auto-generated method stub
		int index =  m_NodeList.indexOf(IdentifyNodeForKey(HashedKey));
		String msgToSend = SINGLEDELETE+":"+key+"\n";

		int i = 0;
		while(i<3)
		{
			int portnumber = m_NodeList.get(index+i);

			if (portnumber == m_selfPortNumber)
			{
				Log.e(TAG, "single delete done");
				Deletekey(key);
			}
			else
			{
				Log.e(TAG, "deletepass");
				SendMethod(msgToSend,portnumber);
			}
			i++;
		}
	}

	public void StarDeleteHandler()
	{

		String msgToSend = ATDELETE + "\n";
		for(int i = 0; i<m_NodeList.size();++i)
		{
			int portnumber = m_NodeList.get(i);
			if(portnumber==m_selfPortNumber)
			{
				m_helper.Delete("@");
			}
			else
			{
				SendMethod(msgToSend,portnumber);
			}
		}
	}

	public void Deletekey(String key)
	{
		m_helper.Delete(key);
	}


	@Override
	public  Uri insert(Uri uri, ContentValues values)
	{
		synchronized (synctoken)
		{
            if(m_IsReoveryTaskCreated)
            {

                try {
                    wait();
                } catch (InterruptedException e) {
                    Log.e(TAG, "InterruptedException");
                }

            }
			String key = values.getAsString("key");
			String val = values.getAsString("value");
			Log.e(TAG, "Insert Enter first time:" + key);
			String HashedKey = GenerateHashWrapper(key);
			validateAndInsert(HashedKey, key, val);
			return uri;
		}
	}


	public void validateAndInsert(String HashedKey,String key,String val)
	{
		// TODO Auto-generated method stub

		int index =  m_NodeList.indexOf(IdentifyNodeForKey(HashedKey));
		String startdevicenum = "";
        String version = String.valueOf(0);

		int i = 0;
		while(i<3)
		{
			int portnumber = m_NodeList.get(index+i);

			if(startdevicenum.isEmpty())
			{
				startdevicenum = String.valueOf(portnumber/2);
			}
			if(portnumber == m_selfPortNumber)
			{
				Log.e(TAG, "insertpass--self"+String.valueOf(portnumber)+":"+key+":"+val+":"+String.valueOf(startdevicenum));
				InsertKey(key,val,String.valueOf(startdevicenum));
			}
			else
			{

				String msgToSend = INSERT+":"+key+":"+val+":"+startdevicenum+"\n";
				Log.e(TAG, "insertpass--other"+String.valueOf(portnumber)+":"+msgToSend);
				SendMethod(msgToSend,portnumber);
			}
			i++;
		}

	}


	public void InsertKey(String key,String val,String deviceid)
	{
		Log.e(TAG, "insertkey method enter ");
		Log.v("insert", key+":"+val+":"+deviceid);
		m_helper.Insert(key,val,deviceid);
		Log.e(TAG, "insertkey method exit");

	}

	public String ExtractValuesFromCursor(Cursor cursor,boolean isRecovery ,boolean isSingleQuery)
	{

		Log.e(TAG,"ExtractValuesFromCursor cnt = "+String.valueOf(cursor.getCount()));
		StringBuilder builder = new StringBuilder();
		try
		{

			while (cursor.moveToNext())
			{
				int keyIndex = cursor.getColumnIndex("key");
                String returnKey = cursor.getString(keyIndex);

				int valueIndex = cursor.getColumnIndex("value");
                String returnValue = cursor.getString(valueIndex);


				builder.append(returnKey);
				builder.append("#");
				builder.append(returnValue);

                if(isSingleQuery)
                {
                    int valueversion = cursor.getColumnIndex("version");
                    String returnversion = cursor.getString(valueversion);
                    builder.append("#");
                    builder.append(returnversion);
                }

				if(isRecovery)
				{

                    int valuedeviceid = cursor.getColumnIndex("deviceid");
                    String returndeviceid = cursor.getString(valuedeviceid);

                    int valueversion = cursor.getColumnIndex("version");
                    String returnversion = cursor.getString(valueversion);


                    builder.append("#");
					builder.append(returndeviceid);

                    builder.append("#");
                    builder.append(returnversion);
				}


				if(!cursor.isLast())
				{
					builder.append("#");
				}
			}
		}
		finally
		{
			cursor.close();
		}
		Log.e(TAG,"ExtractValuesFromCursor --- END");
		return builder.toString();
	}


	public Cursor PutStringValuesIntoCursor(String inputstr ,Cursor currentnodecursor)
	{

		MatrixCursor matrixCursor = new MatrixCursor(new String[] {"key","value"});
		Log.e(TAG,"PutStringValuesIntoCursor---inputstr = "+inputstr);


		String[] arr = inputstr.split("#");
		Log.e(TAG, "PutStringValuesIntoCursor---arr len  = " + String.valueOf(arr.length));

		for (int i = 0; i <= arr.length - 2; i = i + 2)
		{
			matrixCursor.addRow(new Object[]{arr[i], arr[i + 1]});
		}

		MergeCursor mergeCursor = new MergeCursor(new Cursor[]{matrixCursor,currentnodecursor});

		Log.e(TAG,"PutStringValuesIntoCursor---mergeCursor len  = "+String.valueOf(mergeCursor.getCount()));
		return mergeCursor;
	}


	public void InsertRecoveryvalues(String inputstr)
	{
        Log.e(TAG, "RecoveryInsert");
        m_helper.RecoveryInsert(inputstr);
	}



	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
						String sortOrder) {
		// TODO Auto-generated method stub

        synchronized (synctoken) {

            if(m_IsReoveryTaskCreated)
            {

                    try {
                        wait();
                    } catch (InterruptedException e) {
                        Log.e(TAG, "InterruptedException");
                    }

            }

            Log.e(TAG, "Query  enter first time : selection = " + selection);
            String HashedKey = "";
            Cursor returncursor = null;

            if (selection.compareTo("@") == 0) {
                returncursor = m_helper.Query(selection);
            } else if (selection.compareTo("*") == 0) {
                returncursor = StarQueryHandler();
            } else {
                HashedKey = GenerateHashWrapper(selection);
                returncursor = SingleQueryHandler(selection, HashedKey);
            }

            return returncursor;
        }
	}


	public Cursor SingleQueryHandler(String selection,String HashedKey)
	{
		// TODO Auto-generated method stub
		String msgToSend ="";
		String strReply = "";

		Cursor returncursor = null;
		int portnumber = IdentifyNodeForKey(HashedKey);

        ArrayList<String> arrl=  new ArrayList<String>();


        int index = m_NodeList.indexOf(portnumber);
        int i = 0;
        int maxversion = 0;
        String maxvalue = "";

        while(i<3) {
            int portnum = m_NodeList.get(i + index);

            if (m_selfPortNumber == portnum) {
                returncursor = m_helper.Query(selection);
                strReply = ExtractValuesFromCursor(returncursor, false, true);
            }
            else
            {
                msgToSend = SINGLEQUERY + ":" + selection + "\n";
                strReply = SendMethod(msgToSend, portnum);

            }
            if(null != strReply && !strReply.isEmpty())
            {
                arrl.add(strReply);
            }
            i++;
        }

        for(int j = 0;j<arrl.size();++j)
        {
            if (arrl.get(j) != null)
            {
                Log.e(TAG,"str="+arrl.get(j));
                String[] strarr = arrl.get(j).split("#");
                int version = Integer.valueOf(strarr[2]);
                if(maxversion < version) {
                    Log.e(TAG, "Strreply" +":"+strarr[0]+":"+strarr[1]+":"+strarr[2]);
                    maxversion = version;
                    maxvalue = strarr[1];
                }
            }

        }

        MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});
        Log.e(TAG,"maxvalue:"+maxvalue);
        matrixCursor.addRow(new Object[]{selection,maxvalue});
        returncursor = matrixCursor;
        Log.e(TAG, "count of m_ReturnCursor =" + String.valueOf(returncursor.getCount()));

		return returncursor;
	}


	public Cursor StarQueryHandler()
	{

		String msgToSend = ATQUERY+"\n";
		String strReply = "";
		Cursor returncursor = m_helper.Query("@");

		for(int i = 0; i<m_NodeList.size();++i)
		{
			int portnumber = m_NodeList.get(i);
			if(portnumber != m_selfPortNumber)
			{
				strReply = SendMethod(msgToSend,portnumber);
				if(strReply != null)
                {
					returncursor = PutStringValuesIntoCursor(strReply, returncursor);
				}
			}
		}

		return  returncursor;
	}



	@Override
	public boolean onCreate()
	{
		TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		String SelfPortNumber = String.valueOf((Integer.parseInt(portStr) * 2));
        m_selfPortNumber = Integer.valueOf(SelfPortNumber);



		devicemap = new HashMap<Integer, String>() ;
		m_NodeList = new CircularList<Integer>();

		for(int i = 0 ;i<5;++i)
		{
			int deviceid = AVD0_ID + i*2;
		   	devicemap.put(deviceid,GenerateHashWrapper(String.valueOf(deviceid)));
		}


		m_NodeList.add(AVD2_PORT);
		m_NodeList.add(AVD3_PORT);
		m_NodeList.add(AVD4_PORT);
		m_NodeList.add(AVD1_PORT);
		m_NodeList.add(AVD0_PORT);



		for(int i = 0 ;i<m_NodeList.size();++i)
		{
			Log.e(TAG, String.valueOf(m_NodeList.get(i)));
		}


		Log.e(TAG, SelfPortNumber);
		try
		{
			Context context = getContext();
			m_helper = new Databasehelper(context);
			Log.v("DB", "Databasehelper created");
		}
		catch (Exception e)
		{
			Log.v("DB", "Databasehelper is not created.Exception Occurred");
		}

		try
		{
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

		} catch (IOException e) {
			Log.e(TAG, "Can't create a ServerSocket");

		}

		if(m_helper.TestTableQueryCount()>0)
		{

			Log.e(TAG,"TestTableQueryCount="+String.valueOf(m_helper.TestTableQueryCount()));
			//Node has failed and Recovered .Data has to be recovered
			new NodeRecoveryTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, SelfPortNumber);

		}
		else
		{
			Log.e(TAG,"TestTableQueryCount="+String.valueOf(m_helper.TestTableQueryCount()));
			m_helper.TestTableInsert();
		}

		return false;

	}


	public String SendMethod(String msgToSend,int PortNumber)
	{

		String replystr = "";

		try {

				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), PortNumber);
				Log.e(TAG, "SendMethod" + String.valueOf(PortNumber));


				Log.e(TAG, "Check1");
				OutputStreamWriter oswriter = new OutputStreamWriter(socket.getOutputStream());
				BufferedWriter bwriter = new BufferedWriter(oswriter);
				bwriter.write(msgToSend);
				bwriter.flush();


				Log.e(TAG, "Check2");
				InputStreamReader isreader = new InputStreamReader(socket.getInputStream());
				BufferedReader breader = new BufferedReader(isreader);
			    replystr = breader.readLine();

				if (replystr != null)
				{
					Log.e(TAG, "reply received");
					bwriter.write("PA3-OK"+"\n");
					bwriter.flush();
				}
				else
				{
					Log.e(TAG, "NULL received -- failed devic port number = "+String.valueOf(PortNumber));
				}

				bwriter.close();
				breader.close();
				socket.close();
			} catch (UnknownHostException e) {
				Log.e(TAG, "ClientTask UnknownHostException");
			} catch (IOException e) {

			}

			return replystr;
	}


	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		protected  Void doInBackground(ServerSocket... sockets)
		{

            Log.e(TAG, "ServerTask Enter");
            ServerSocket serverSocket = sockets[0];
            /*
             * TODO: Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             */
				while (true)
                {

                    if(m_IsReoveryTaskCreated)
                    {
                        synchronized (synctoken) {
                                 try {
                                    wait();
                                } catch (InterruptedException e) {
                                    Log.e(TAG, "InterruptedException");
                                }

                        }
                    }

					Log.e(TAG, "Serve0r Task in Before While:" + String.valueOf(m_selfPortNumber));

					try {
						Socket soc = serverSocket.accept();
						Log.e(TAG, "Server Task check 1");

						InputStreamReader isreader = new InputStreamReader(soc.getInputStream());
						BufferedReader breader = new BufferedReader(isreader);
						String datastrrxed = breader.readLine();

						Log.e(TAG, "Server Task check 2");
						Log.e(TAG, datastrrxed);
						String datastrtobesent = "";


						if (datastrrxed != null) {
							Log.e(TAG, "Server Task in datastr if ");
							datastrrxed = datastrrxed.replace("\n", "");
							String[] arr = datastrrxed.split(":");

							if (arr.length == 4 && arr[0].compareTo(INSERT) == 0)
							{
								Log.e(TAG, "INSERT");
								InsertKey(arr[1], arr[2],arr[3]);
							}
							else if (arr.length == 2 && arr[0].compareTo(SINGLEQUERY) == 0)
							{
								Log.e(TAG, "SINGLEQUERY");
								Cursor cursor = m_helper.Query(arr[1]);
								datastrtobesent = ExtractValuesFromCursor(cursor,false,true);
                                Log.e(TAG,SINGLEQUERY+":"+datastrtobesent);

							}
						    else if (arr.length == 1 && arr[0].compareTo(ATQUERY) == 0)
						    {
							     Log.e(TAG, "ATQUERY");
								 Cursor cursor = m_helper.Query("@");
								 Log.e(TAG, "ATQUERYPASS -- ELSE Cursor Count = " + String.valueOf(cursor.getCount()));
								 datastrtobesent = ExtractValuesFromCursor(cursor,false,false);

							}
							else if (arr.length == 2 && arr[0].compareTo(SINGLEDELETE) == 0)
							{
								Deletekey(arr[1]);
							}
							else if (arr.length == 1 && arr[0].compareTo(ATDELETE) == 0)
							{
								m_helper.Delete("@");
							}
							else if (arr.length == 3 &&arr[0].compareTo(RECOVERY)==0)
							{
								boolean flag = false ;
								if(arr[2].compareTo("empty")==0)
								{
									flag =true;
								}
								Cursor cursor = m_helper.RecoveryQuery(flag,arr[1],arr[2]);
						        datastrtobesent = ExtractValuesFromCursor(cursor,true,false);
							}

						} else {
							Log.e(TAG, "Data is not received");
						}


						OutputStreamWriter oswriter = new OutputStreamWriter(soc.getOutputStream());
						BufferedWriter bwriter = new BufferedWriter(oswriter);
						bwriter.write(datastrtobesent+"\n");
						bwriter.flush();

						InputStreamReader ireader2 = new InputStreamReader(soc.getInputStream());
						BufferedReader breader2 = new BufferedReader(isreader);
						String ackstring = breader.readLine();

						if (ackstring != null && ackstring.equals("PA3-OK"+"\n"))
						{
							Log.e(TAG, "PA3-OK received");
						}
						else
						{
							Log.e(TAG, "NULL received");
						}

						Log.e(TAG, "Server Task check 3");
						bwriter.close();
						breader.close();
						breader2.close();
						soc.close();

					}
					catch (IOException e)
					{
						Log.e(TAG, "Server Task socket IOException");
					}

				}


		}

	}

	private class NodeRecoveryTask extends AsyncTask<String,Void,Void> {

		@Override
		protected Void doInBackground(String... msgs)
		{
			Log.e(TAG, "NodeRecoveryTask start");


			int index = m_NodeList.indexOf(m_selfPortNumber);

			int RecoveryPort1 = m_NodeList.get(index+1);
			int RecoveryPort2 = m_NodeList.get(index-1);
			int RecoveryPort3 = m_NodeList.get(index-2);


			String deviceidvalue1 = String.valueOf(m_selfPortNumber/2);
			String deviceidvalue2 = String.valueOf(RecoveryPort2/2);
			String deviceidvalue3 = String.valueOf(RecoveryPort3/2);


			String msgToSend = RECOVERY+":"+deviceidvalue1+":"+"empty"+"\n";
			String replystr1 = SendMethod(msgToSend,RecoveryPort1);
            InsertRecoveryvalues(replystr1);


			msgToSend = RECOVERY+":"+deviceidvalue2+":"+deviceidvalue3+"\n";
			String replystr2 = SendMethod(msgToSend,RecoveryPort2);
			InsertRecoveryvalues(replystr2);


            synchronized (synctoken) {
                synctoken.notify();
            }

            m_IsReoveryTaskCreated = false;
            Log.e(TAG, "NodeRecoveryTask exit");

            return null;
		}

	}

	private String GenerateHashWrapper(String input)
	{
		String hashstr = "";
		try
		{
			hashstr = genHash(input);
		}
		catch (NoSuchAlgorithmException e)
		{
			Log.e(TAG,"NoSuchAlgoritmException");
		}
		return hashstr;
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



		@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}


}
