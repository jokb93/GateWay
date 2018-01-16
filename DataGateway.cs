using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using IoTLib.BackgroundServices;
using IoTLib.Interfaces;
using Newtonsoft.Json.Linq;
using System.Threading;
using System.Diagnostics;
using System.Collections.Concurrent;
using IoTLib.Objects;
using System.Data.SQLite;
using MongoDB.Bson;
using System.Data;

namespace IoTLib
{
    public class DataGateway
    {

        private IDatabase Database;
        private bool pause = false;
        private List<BsonDocument> PLCConnections;
        private SQLiteConnection MiddleDB;
        private ConcurrentQueue<LogObject> Queue = new ConcurrentQueue<LogObject>();
        private bool flag = false;

        #region Startup Modules
        public DataGateway()
        {
            
            try
            {
                Database = DBHandler.getDB(config.ServerURI, config.AuthKey, config.DB, config.SSL);
                MiddleDB = new SQLiteConnection("Data Source=AntiDataLoss.sqlite;Version=3;");
                MiddleDB.Open();
            }
            catch
            {
            }

            GetPLCsToConnect();
            
            Thread AnitLossThread = new Thread(() =>
            {
                ADL(ref Queue);
            });
            AnitLossThread.Name = "ADLThread";
            AnitLossThread.Start();
            
            Thread WriteThread = new Thread(() =>
            {
                WriteModule();
            });
            WriteThread.Name = "WriteThread";
            WriteThread.Start();

        }

        private void GetPLCsToConnect()
        {
            PLCConnections = Database.GetCollection("Config", "Connections", config.GatewayId, "Site");
            Database.CreateCollection("Monitor", "RequestTime", 30000000);


            foreach (BsonDocument PLCDoc in PLCConnections)
            {
                IPlc PLC = PlcHandler.getClient(PLCDoc["PLCType"].ToString());
                PLC.ClientDetails = PLCDoc;
                PLC.Connect();

                Thread ClientThread = new Thread(() =>
                {
                    RunConnection(PLC, Database);
                });

                ClientThread.Name = PLC.ClientDetails["Name"].ToString();
                ClientThread.Start();
            }

        }


        private void RunConnection(IPlc PLC, IDatabase Database)
        {
            
            long ReqTime = 10;
            List<BsonDocument> Datastreams = UpdateDatastreams(PLC, Database);
            List<BsonDocument> Tempstreams = UpdateDatastreams(PLC, Database);

            Action<int, dynamic> Callback = new Action<int, dynamic>((Code, opts) =>
            {

                switch (Code)
                {
                    case 0:
                        Datastreams = Tempstreams;
                        break;
                    case 1:
                        Tempstreams = opts;
                        break;
                    default:
                        break;
                }

            });

            Thread ReadThread = new Thread(() =>
            {
                ReadModule(ref Datastreams, ref ReqTime, Callback, PLC);
            });
            ReadThread.Name = PLC.ClientDetails["Name"].ToString() + " - ReadThread";

            Thread UpdateThread = new Thread(() =>
            {
                UpdateModule(PLC,ref ReqTime,Callback);
            });
            UpdateThread.Name = PLC.ClientDetails["Name"].ToString() + " - UpdateThread";

            UpdateThread.Start();
            ReadThread.Start();
            
        }
        #endregion

        #region Main modules

        #region Read Modules
        private void ReadModule(ref List<BsonDocument> Datastreams, ref long ReqTime, Action<int, dynamic> Callback, IPlc PLC)
        {
            long TimeStamp = (long)(DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalMilliseconds;
            LogObject logObj;
            Stopwatch watch = new Stopwatch();

            foreach (BsonDocument Stream in Datastreams)
            {
                Stream["oldTime"] = 0;
            }
            Callback(1, Datastreams);

            watch.Start();
            long Req = 0;

            while (true)
            {
                Callback(0, null);
                TimeStamp = (long)(DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalMilliseconds;
                watch.Restart();
                foreach (BsonDocument Stream in Datastreams)
                {
                    try
                    {
                        if (BsonResolver.Resolve(Stream["Active"]) && BsonResolver.Resolve(Stream["Interval"]) <= TimeStamp - BsonResolver.Resolve(Stream["oldTime"]))
                        {

                            byte[] buffer = GetBuffer(BsonResolver.Resolve(Stream["DataType"]));
                            PLC.Read(Stream, ref buffer);

                            logObj = new LogObject();
                            logObj.PLC = BsonResolver.Resolve(PLC.ClientDetails["id"].ToString());
                            logObj.Datastream = BsonResolver.Resolve(Stream["Collection"]) + "_" + BsonResolver.Resolve(Stream["id"]);
                            logObj.Value = buffer;
                            logObj._ts = TimeStamp;
                            logObj.DataType = BsonResolver.Resolve(Stream["DataType"]);
                            logObj.Position = BsonResolver.Resolve(Stream["Position"]);

                            Queue.Enqueue(logObj);

                            Stream["oldTime"] = TimeStamp;
                        }

                    }
                    catch
                    {
                        PLC.Disconnect();
                        while (!PLC.Connect())
                        {
                            Thread.Sleep(1000);
                        }
                    }
                }
                Req = watch.ElapsedMilliseconds;
                if (Req > 0)
                {
                    ReqTime = Req;
                }

            }
        }

        private byte[] GetBuffer(int DataType)
        {
            int Size = 0;
            switch (DataType)
            {
                case 1:
                    Size = 2;
                    break;
                case 2:
                    Size = 256;
                    break;
                case 3:
                    Size = 8;
                    break;
                case 4:
                    Size = 1;
                    break;
                case 5:
                    Size = 4;
                    break;
                default:
                    Size = 4;
                    break;
            };

            return new byte[Size];
        }
        #endregion
        
        #region Write Modules
        private void WriteModule()
        {

            Stopwatch watch = new Stopwatch();
            watch.Start();
            while (true)
            {

                
                Thread.Sleep(500);
                if (watch.ElapsedMilliseconds > 2500)
                {
                    watch.Restart();
                }
                while (pause){}
                writeData(watch.ElapsedMilliseconds);

            }

        }

        private void writeData(long watch)
        {
            Dictionary<string, List<BsonDocument>> Scope = new Dictionary<string, List<BsonDocument>>();

            Dictionary<string, List<BsonDocument>> Reads = SQLiteRead(watch);
            
            int Inserted = 0;
            foreach (KeyValuePair<string, List<BsonDocument>> Logs in Reads)
            {
                try
                {
                    //Inserter ingen hvis den slår fejl, laver et rollback i IDatabase, hvis ikke alle kunne indsættes.
                    Inserted += Database.MultiInstert("Data", Logs.Key, Logs.Value);
                }
                catch
                {

                }

            }

            string sql = "Delete from Data where Id IN (Select Id from Data limit "+Inserted+");";
            SQLiteCommand command = new SQLiteCommand(sql, MiddleDB);
            command.ExecuteNonQuery();
        }

        private Dictionary<string, List<BsonDocument>> SQLiteRead(long timeElapsed)
        {
            Dictionary<string, List<BsonDocument>> insertPair = new Dictionary<string, List<BsonDocument>>();
            try
            {
                string sql = "select * from Data order by Datastream desc";
                SQLiteCommand command = new SQLiteCommand(sql, MiddleDB);
                SQLiteDataReader reader = command.ExecuteReader();
                List<BsonDocument> Docs = null;
                string oldDS = null;
                string id = null;
                while (reader.Read())
                {
                    id = reader["PLC"].ToString();
                    byte[] byteArray = Convert.FromBase64String(reader["Value"].ToString());
                    if (!insertPair.ContainsKey(reader["Datastream"].ToString()) && oldDS != reader["Datastream"].ToString())
                    {
                        if (Docs != null) {
                            insertPair.Add(oldDS, Docs);
                        }
                        if (timeElapsed > 2000)
                        {
                            Database.DocUpdate("Config", "Connections", JObject.Parse("{Ping: " + (long)(DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalMilliseconds + "}"), id);
                        }
                        Docs = new List<BsonDocument>();
                    }

                    int type = (int)reader["DataType"];
                    int pos = (int)reader["Position"];
                    long timestamp = Convert.ToInt64(reader["_ts"]);

                    Docs.Add(new BsonDocument{
                                { "Value", DataResolver.ByteArrayConverter(byteArray, type, pos) },
                                { "_ts", timestamp }
                            });
                    oldDS = reader["Datastream"].ToString();

                }
                if (Docs != null)
                {
                    insertPair.Add(oldDS, Docs);
                } else
                {

                }
                
            }
            catch (Exception e)
            {
                if (flag)
                {
                    string sql = "Delete from Data where Id IN (Select Id from Data limit 1);";
                    SQLiteCommand command = new SQLiteCommand(sql, MiddleDB);
                    command.ExecuteNonQuery();
                    flag = false;
                } else
                {
                    flag = true;
                }
            }
            return insertPair;
        }
        #endregion

        #endregion

        #region Sub modules

        #region AntiDataLoss
        public void ADL(ref ConcurrentQueue<LogObject> Queue)
        {
            string sql = "create table Data (Id INTEGER PRIMARY KEY,PLC varchar(256), Datastream varchar(256), _ts TEXT, DataType int, Position int, Value TEXT)";
            SQLiteCommand cmd = new SQLiteCommand(sql, MiddleDB);
            try
            {
                cmd.ExecuteNonQuery();
            }
            catch (Exception)
            {

            }
            while (true)
            {
                LogObject obj;
                Thread.Sleep(1000);
                int Amount = Queue.Count;
                for (int i = 0; i < Amount - 1; i++)
                {
                    Queue.TryDequeue(out obj);
                    string val = Convert.ToBase64String(obj.Value);
                    cmd.CommandText = "INSERT INTO Data (PLC, Datastream, _ts, DataType, Position, Value) VALUES ('" + obj.PLC + "', '" + obj.Datastream + "', '" + obj._ts.ToString() + "', " + obj.DataType + ", " + obj.Position + ", '" + val + "');";

                    cmd.ExecuteNonQuery();
                }
            }
        }
        #endregion

        #region Update Module

        public void UpdateModule(IPlc PLC, ref long ReqTime , Action<int, dynamic> Callback)
        {
            int runs = 0;
            while (true)
            {
                Thread.Sleep(5000);
                List<BsonDocument> req = new List<BsonDocument>();
                try
                {
                    req.Add(BsonDocument.Parse("{ time : " + ReqTime + ", ClientId : '" + PLC.ClientDetails["id"].ToString() + "', Site : '" + PLC.ClientDetails["Site"].ToString() + "',_ts : " + (long)(DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalMilliseconds + "}"));
                    Database.MultiInstert("Monitor", "RequestTime", req);
                }
                catch
                {

                }

                runs++;
                if(runs == 20)
                {
                    List<BsonDocument> temp = UpdateDatastreams(PLC, Database);
                    foreach (BsonDocument Stream in temp)
                    {
                        Stream["oldTime"] = 0;
                    }
                    Callback(1, temp);
                    runs = 0;
                }

            }

        }
        
        public List<BsonDocument> UpdateDatastreams(IPlc PLC, IDatabase Database)
        {
            List<BsonDocument> Temp = Database.GetCollection("Config", "DataStreams", PLC.ClientDetails["id"].ToString(), "ConnectionId");
            Temp = Temp.Where(Doc => BsonResolver.Resolve(Doc["Active"])).Distinct().ToList();

            List<string> Sets = Database.GetCollections("Data");

            foreach (BsonDocument Stream in Temp)
            {
                if (!Sets.Contains(BsonResolver.Resolve(Stream["Collection"])))
                {
                    Database.CreateCollection(BsonResolver.Resolve(Stream["Collection"]) + "_" + BsonResolver.Resolve(Stream["id"]), "Data");
                }
            }

            return Temp;
        }

        #endregion

        #endregion

        #region Misc
        private int GetMaxInterval(List<JObject> Datastreams)
        {

            int Max = 0;

            foreach (JObject Stream in Datastreams)
            {
                if (Convert.ToInt32(Stream["Interval"]) > Max)
                {
                    Max = Convert.ToInt32(Stream["Interval"]);
                }
            }

            return Max;

        }
        #endregion

    }
}
