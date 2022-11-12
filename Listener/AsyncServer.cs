using System;
using System.Data;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Globalization;
using System.Messaging;
using EsmeeBO;
using EsmeeDBOperations;
using NLog;
using Newtonsoft.Json;
using System.Data.SqlClient;

namespace Listener
{
    public class Bfr
    {
        private static Logger logger = LogManager.GetCurrentClassLogger();
        int bytesToRead { get; set; }
        public int bytesRead { get; set; }
        public byte[] bytes { get; set; }
        public Bfr()
        {
            bytesToRead = 0;
            bytesRead = 0;
            bytes = new byte[4096];
        }

        public int ReadNBytes(NetworkStream stream, int num)
        {

            int n = 0;
            //logger.Debug($"num = {num} bytesRead = {bytesRead} bytesToRead = {bytesToRead}-{num - bytesRead}");
            bytesToRead = num - bytesRead;
            while (bytesRead < num)
            {
                try
                {
                    n = stream.Read(bytes, bytesRead, bytesToRead);
                    if (n == 0)
                    {
                        logger.Debug("Task {tid} disconnect in ReadNBytes");
                        Thread.Sleep(1000);
                        return 0;
                    }
                    bytesRead += n;
                    bytesToRead -= n;

                    //logger.Debug($"read {n} bytes {bytesRead} {bytesToRead} {string.Join(".", bytes.Take(bytesRead).Select(x => x.ToString("X2")).ToArray())}");
                }
                catch (Exception ex)
                {
                    if (ex.HResult == -2146232800)
                    {
                        logger.Debug("Task {tid} Timeout on read");
                    }
                    else
                    {
                        logger.Debug("Task {tid} error in ReadNBytes {ex}");
                    }
                    return  0;
                }
            }

            return bytesRead;
        }
    }
    public class AsyncServer
    {
        private static Logger logger = LogManager.GetCurrentClassLogger();
      
        private static String[] queuenames = new string[40];
        private  ConcurrentDictionary<string, StateObject> connectedImei = new ConcurrentDictionary<string, StateObject>();
        public SqlClientUtility mainConnection { get; set; }
        private static int nqueues;
        private int listenPort { get; set; }
        public int? tid = 0;
      
        private bool GetVer = Properties.Settings.Default.GetVer;
        //private bool Codec12Translate = Properties.Settings.Default.Codec12Translate;
        public AsyncServer(int port)
        {
            listenPort = port;
        }

        public async Task Start()
        {
            listenPort = Convert.ToInt32(Properties.Settings.Default.Port);
            try
            {
                mainConnection = new SqlClientUtility(Properties.Settings.Default.DBConnection);
            }
            catch (Exception ex)
            {
                throw new ArgumentException("Error connecting to sql server: " + ex.Message);
            }
            nqueues = Convert.ToInt32(Properties.Settings.Default.NumberOfQueues);
            String quen = Properties.Settings.Default.MSMQStore;
            String quen2 = Properties.Settings.Default.MSMQStore2;
            for (int i = 0; i < nqueues; i++)
            {
                queuenames[i] = quen + "_" + i.ToString();
                queuenames[i + nqueues] = quen2 + "_" + i.ToString();
                if (!MessageQueue.Exists(@".\Private$\" + queuenames[i]))
                {
                    var myQueue = MessageQueue.Create(".\\Private$\\" + queuenames[i], false);
                    myQueue.SetPermissions("Everyone", MessageQueueAccessRights.FullControl);
                }
                if (!MessageQueue.Exists(@".\Private$\" + queuenames[i + nqueues]))
                {
                    var myQueue = MessageQueue.Create(".\\Private$\\" + queuenames[i + nqueues], false);
                    myQueue.SetPermissions("Everyone", MessageQueueAccessRights.FullControl);
                }
            }
            TcpListener listener = new TcpListener(IPAddress.Any, listenPort);
            listener.Start();
            while (true)
            {
                TcpClient tcpClient = await listener.AcceptTcpClientAsync();
#pragma warning disable 4014
                Task.Run(() => Process(tcpClient));
            }
        }
        
        private void Process(TcpClient client)
        {
            tid = Task.CurrentId;
            StateObject so = new StateObject() ;
            var ip = client.Client.RemoteEndPoint.ToString();
            var stream = client.GetStream();
            stream.ReadTimeout = 4500000;
            var connected = false;
            int length;
            Byte[] ack = new Byte[4];
            
            logger.Info($"task {tid} Received connection from {ip}", ip);
            try
            {
                Bfr bfr = new Bfr();
                while ((length = bfr.ReadNBytes(stream, 1)) > 0)
                {

                   // length = await bfr.ReadNBytes(stream, 1);
                    if (length == 0) break;
                    if (bfr.bytes[0] == 0)
                    {
                        //int numBytesToRead = 0;
                        //int numBytesRead = 1;
                        if (!connected)
                        {
                            //so = new StateObject();
                            so.timer = new Timer(new TimerCallback(OnTimer), (Object)so, 4500000, Timeout.Infinite); //5 kwartier
                            so.tcpClient = client;
                            so.ip = ip;
                            //numBytesRead = length;
                            //numBytesToRead = 17 - numBytesRead;
                            int nn = bfr.ReadNBytes(stream, 17);
                            if (nn == 0 ) {
                                break;
                            }
                            logger.Debug($"Task {tid} Received {ip} {string.Join(".", bfr.bytes.Take(bfr.bytesRead).Select(x => x.ToString("X2")).ToArray())}");
                            if (getImei(bfr.bytes, 17, so) == 0)
                            {
                               return;
                            }
                            connected = true;
                            if (GetVer)
                            {
                                Codec12 c = new Codec12("getver", so.imei);
                                string getver = JsonConvert.SerializeObject(c);
                                byte[] getvera = Encoding.ASCII.GetBytes(getver);
                                sendCodec12(getvera, getvera.Length, so);
                            }
                            bfr = new Bfr();
                        }
                        else
                        {
                            //numBytesRead = length;
                            //numBytesToRead = 8 - numBytesRead;
                            int nn = bfr.ReadNBytes(stream, 8);
                            if (nn == 0)
                            {
                                break;
                            }
                            if (bfr.bytes[1] == 15)
                            {
                                logger.Info($"Task {tid} imei received while connected on {ip}");
                                 nn = bfr.ReadNBytes(stream, 17);
                                if (getImei(bfr.bytes, 17, so) == 0)
                                {
                                    return;
                                }
                                if (nn == 0)
                                {
                                    break;
                                }
                                bfr = new Bfr();
                                continue;
                            }
                            so.timer.Dispose();
                            new Timer(new TimerCallback(OnTimer), (Object)so, 4500000, Timeout.Infinite); //5 kwartier
                            int avlLength = BytesToNum(bfr.bytes, 4, 4) + 8 + 4;
                            nn = bfr.ReadNBytes(stream, avlLength);
                            if (nn == 0)
                            {
                                break;
                            }
                            logger.Debug($"Task {tid} Received {so.imei} {string.Join(".", bfr.bytes.Take(bfr.bytesRead).Select(x => x.ToString("X2")).ToArray())}");
                            int codec = bfr.bytes[8];
                            if (codec == 8 || codec == 0x8e)
                            {
                                nn = getCodec8(bfr.bytes, avlLength, so);
                                if (nn == 0 )
                                {
                                    break;
                                }
                            }
                            else if (codec == 12)
                            {
                                nn = getCodec12(bfr.bytes, avlLength, so);
                                if (nn == 0)
                                {
                                    break;
                                }
                            }
                            else
                            {
                                logger.Error($"Task {tid} Codec {codec} not handled");
                                int temp = bfr.bytes[9];
                                Byte[] newack = new Byte[4];
                                for (int i = 3; i >= 0; i--)
                                {
                                   newack[i] = (byte)(temp & 0xff);
                                   temp = temp >> 8;
                                }
                                so.tcpClient.GetStream().Write(newack, 0, 4);
                                logger.Info($"Task {tid} Response {so.ip} {string.Join(".", newack.Select(x => x.ToString("X2")).ToArray())}");
                                return;
                            }
                        }
                        bfr = new Bfr();
                    }
                    else
                    {
                         length = stream.Read(bfr.bytes, 1, 4095);
                          
                        if (bfr.bytes[0] == '{')
                        {
                            so = new StateObject();
                            so.tcpClient = client;
                            sendCodec12(bfr.bytes, length + 1, so);
                        }
                        else
                        {
                            getStepp1(bfr.bytes, length, so);

                        }
                        bfr = new Bfr();
                    }
                }
                logger.Info($"Task {tid}Connection {ip} lost");
                CloseSocket(so);
                return;
            }
            catch (Exception ex)
            {
                logger.Info($"Task {tid} Socket {ip} closed {ex} (no longer available)");
                CloseSocket(so);
            }
        }
        private int getImei(byte[] buffer, int len, StateObject so)
        {
            Byte[] ack = new Byte[4];
            int lena = buffer[1];
            StringBuilder sb = new StringBuilder();
            for (int i = 2; i < 2 + lena; i++)
            {
                sb.Append((char)buffer[i]);
            }
            so.imei = sb.ToString();
            so.gpsSerial = "";
            
            try {
                GpsBoxDBO gpsDBO = new GpsBoxDBO();
                //var gpsFromDb = gpsDBO.SelectGpsByImei(so.imei, mainConnection.Connection);
                var gpsFromDb = SelectGpsByImei2(so.imei);
                CloseDB();
                //If the serial is not found in the database, drop the information
                if (gpsFromDb == null)
                {
                    //logger.Error(" imei " + so.imei + " not found in db");
                    // send nack
                    ack[0] = 0;
                    so.tcpClient.GetStream().Write(ack, 0, 1);
                    CloseSocket(so);                    
                    throw new ArgumentException($"Task {tid} Box not found: " + so.imei);
                }
                else 
                {
                    so.gpsSerial = gpsFromDb.Serial;
                    so.boxTypeName = gpsFromDb.boxtype;
                }
                
            }
            
            catch (Exception ex)
            {
                logger.Error(" imei " + so.imei + " error " + ex.Message);
                return 0;
            }
            
            // first data from fm2200
            logger.Info($"Task {tid} new connection ip " + so.ip + " imei " + so.imei);
            
            if (connectedImei.ContainsKey(so.imei))
            {
                StateObject sotemp = connectedImei[so.imei];
                CloseSocket(sotemp);
                //connectedImei.Remove(sotemp.imei);
                logger.Info($"Task {tid} close {sotemp.imei} {sotemp.ip} in connection list");
            }
            /*
            else
            {
                //logger.Info("imei {0} not in connection list", so.imei);
            }
            //logger.Info("add imei {0} {1} to connection list", so.imei, so.ip);
            connectedImei.Add(so.imei, so);
            */
            logger.Info($"add or update imei {so.imei} {so.ip} in connection list");
            connectedImei[so.imei] = so;
            ack[0] = 1;
            logger.Info($"Task {tid} Response {so.ip} 01");
            so.tcpClient.GetStream().Write(ack, 0, 1);
              return 1;
        }
        
        public int getCodec8(Byte[] buffer, int len, StateObject so)
        {
            try
            {
                Decode.Payload p = new Decode.Payload(buffer, 0, len);
                int numRecs = buffer[9];
                int tcpheader = p.readInt(4);
                int AVLpktLen = p.readInt(4);
                int CodecID = p.readInt(1);  //08 if FM4XXX
                byte NumOfRecords = (byte)p.readInt(1);
                for (int i = 0; i < NumOfRecords; i++)
                {                  
                    EntityClass message = new EsmeeBO.EntityClass(p, CodecID);
                    message.Imei = so.imei;
                    
                    if (message.IButton != "")
                    {
                        so.ibutton = message.IButton;
                    }
                    else
                    {
                        if (so.ibutton != "")
                        {
                            message.IButton = so.ibutton;
                        }
                    }
                    message.BoxID = so.gpsSerial;
                    message.BoxTypeName = so.boxTypeName;

                    if (message.BoxTypeName == "FM1200")
                    {
                        if (message.Speed > 0)
                        {
                            message.Engine = true;
                        }
                        else
                        {
                            message.Engine = false;
                        }
                    }
                    if (Canbus(message.BoxTypeName) > 0)
                    {
                        message.Speed = message.Speed81;
                    }
                    // FMB-CAN
                    if (Canbus(message.BoxTypeName) == 3 && message.FuelL > 0)
                    {
                        message.FuelL = (int)((message.FuelL + 5) / 10);
                        message.FuelD = message.FuelL;
                    }
                    //var json = message.ToJson();
                    if (message.Latitude != 0.0M || message.Longitude != 0.0M)
                    {
                        SaveToMsmq(message);
                    }

                }

                // state.msg = new Byte[StateObject.BufferSize];
                // state.msgp = 0;
                int temp = buffer[9];
                Byte[] ack = new Byte[4];
                for (int i = 3; i >= 0; i--)
                {
                    ack[i] = (byte)(temp & 0xff);
                    temp = temp >> 8;
                }
                so.tcpClient.GetStream().Write(ack, 0, 4);
                logger.Info($"Task {tid} Response {so.ip} {string.Join(".", ack.Select(x => x.ToString("X2")).ToArray())}");
            }
            catch (Exception ex)
            {
                logger.Error($"Task {tid} getCodec8 {ex}");
                return 0;
            }
            return 1;
        }
        class Codec12
        {
            public string cmd;
            public string imei;
            public Codec12(string c, string i)
            {
                cmd = c;
                imei = i;
            }
        }
        class Codec12Error
        {
            public string error;
            public Codec12Error(string s)
            {
                error = s;
            }
        }
        class Codec12Reply
        {
            public string data;
            public Codec12Reply(string s)
            {
                data = s;
            }
        }
        public int sendCodec12(Byte[] buffer, int len, StateObject state) {
            try
            {
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < len; i++)
                {
                    sb.Append((char)buffer[i]);
                    
                }
                string cmd = sb.ToString();
                Codec12 c12 = JsonConvert.DeserializeObject<Codec12>(cmd);
                //Hashtable cmds = (Hashtable)JSON.JsonDecode(cmd);
                
                //string stosend = c12.cmd;
                string imei = c12.imei;
                logger.Info(" imei " + imei + "codec12 request: " + c12.cmd);
                StateObject connectedIp = null;

                if (connectedImei.ContainsKey(imei))
                {
                    connectedIp = connectedImei[imei];
                    logger.Info($"Task {tid}  imei " + imei + " is connected on ip " + connectedIp.ip);
                    if (connectedIp.boxTypeName == "FM1100" || Canbus(connectedIp.boxTypeName) > 0)
                    {
                        if (c12.cmd == "#SET OUT1=1")
                        {
                            c12.cmd = "setdigout 1";
                            state.c12reply = "OUT1=1";
                        }
                        else if (c12.cmd == "#SET OUT1=0")
                        {
                            c12.cmd = "setdigout 0";
                            state.c12reply = "OUT1=0";
                        }
                    }
                    Byte[] c12msg = BuildMsg(c12.cmd);
                    connectedIp.tcpClient.GetStream().Write(c12msg, 0 , c12msg.Length);
                    connectedIp.codec12Requestor = state;
                    connectedIp.codec12Requestor.timer = new Timer(new TimerCallback(OnTimer2), (Object)connectedIp, 30000, Timeout.Infinite);
                }
                else
                {
                    logger.Info($"Task {tid} {imei} is not connected");
                    //Hashtable json = new Hashtable();
                    //json["error"] = "not connected";
                    Codec12Error e = new Codec12Error("not connected");
                    var ej = JsonConvert.SerializeObject(e);
                    state.tcpClient.GetStream().Write(System.Text.Encoding.UTF8.GetBytes(ej), 0, ej.Length);
                    //CloseSocket(state);
                }
            }
            catch (Exception ex)
            {
                Hashtable json = new Hashtable();
                //json["error"] = ex.Message;
                // jsons = JSON.JsonEncode(json);
                Codec12Error e = new Codec12Error(ex.Message);
                var ej = JsonConvert.SerializeObject(e);
                state.tcpClient.GetStream().Write(System.Text.Encoding.UTF8.GetBytes(ej), 0, ej.Length);
            }
            return 0;
        }
        public Int32 BytesToNum(Byte[] bits, int s, int c)
        {
            int i, l;
            l = 0;
            for (i = s; i < s + c; i++)
            {
                l = (l << 8) | bits[i];
            }
            return l;
        }
        public int getCodec12(Byte[] buffer, int requestLength, StateObject state)
        {
            try
            {
                int cmdSize = BytesToNum(buffer, 11, 4);
                byte[] reply = new byte[cmdSize];
                for (int i = 0; i < cmdSize; i++)
                {
                    reply[i] = buffer[15 + i];
                }
                string rep = System.Text.UTF8Encoding.UTF8.GetString(reply);
                if (rep.Contains("DOUT"))
                {
                    rep = state.codec12Requestor.c12reply;
                }

                Codec12Reply repo = new Codec12Reply(rep);
                var jsons = JsonConvert.SerializeObject(repo);
                if (state.codec12Requestor != null)
                {
                    if (state.codec12Requestor.timer != null)
                    {
                        state.codec12Requestor.timer.Dispose();
                        state.codec12Requestor.timer = null;
                    }
                    state.codec12Requestor.tcpClient.GetStream().Write(System.Text.Encoding.UTF8.GetBytes(jsons), 0, jsons.Length);
                }
                logger.Info($"Task {tid} codec12 reply" + jsons);
                return 1;
            }
            catch (Exception ex)
            {
                var e = new Codec12Error(ex.Message);
                var ej = JsonConvert.SerializeObject(e);
                state.tcpClient.GetStream().Write(System.Text.Encoding.UTF8.GetBytes(ej), 0, ej.Length);
                return 0;
            }
        }
		
        public int getStepp1(Byte[] result, int requestLength, StateObject state) {
            int len = requestLength;
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i <= len; i++)
            {
                sb.Append((char)result[i]);
            }
            try
            {
                string content = sb.ToString().Replace("^M\n", "").Trim();
                logger.Info("content = |" + content + "|");
                if (content.EndsWith("<end>") ||
                content.EndsWith("<end>\0") ||
                content.EndsWith("<end>\r") ||
                content.EndsWith("<end>\n") ||
                content.EndsWith("<end>\r\n"))
                {
                    string[] sep = { "$<start>" };
                    string[] res = content.Split(sep, StringSplitOptions.RemoveEmptyEntries);
                    for (int i = 0; i < res.Length; i++)
                    {
                        if (res[i].IndexOf("GPIOP") > -1)
                        {
                            Console.WriteLine("<- $<start>" + res[i]);
                            EsmeeBO.EntityClass message = new EsmeeBO.EntityClass(res[i]);
                            message.Imei = message.BoxID;
                            SaveToMsmq(message);
                        }
                    }
                    //SaveToMsmq(System.Text.Encoding.UTF8.GetBytes(content), content.Length);
                    //SaveToMsmq(state.msg, state.msgp);                                   

                }
            }
            catch (Exception ex)
            {
                logger.Error("getStepp1 #(0)", ex.StackTrace);
            }
            return 0;
        }
        public void SaveToMsmq(EntityClass message)
        {       
            try
            {
                MessageQueue myQueue = null;
                Message msg = null;
                int sum = 0;
                /*
                 * *
                string boxid = message.BoxID;
                if (boxid == null || boxid.Length == 0)
                {
                    return;
                }
                
                for (int i = 0; i < boxid.Length; i++) {
                    sum = sum + (int)boxid[i];
                }
                */
                string imei = message.Imei;
                if (imei == null || imei.Length == 0)
                {
                    return;
                }

                for (int i = 0; i < imei.Length; i++)
                {
                    sum = sum + (int)imei[i];
                }
                int qn = sum % (nqueues * 2);
                var json = message.ToJson();
                //Console.WriteLine("=> "+json);
                //Console.WriteLine("=> "+message.ToString());
                byte[] infoBytes = System.Text.Encoding.UTF8.GetBytes(json);
                logger.Info("Payload: q=" + qn + " "  + json);
                try
                {
                    myQueue = new MessageQueue(".\\Private$\\" + queuenames[qn], QueueAccessMode.SendAndReceive);
                    msg = new Message();
                    msg.BodyStream.Write(infoBytes, 0, infoBytes.Length);
                    myQueue.Send(msg);
                }
                catch (Exception ex)
                {
                    logger.Error("Create message queue " + ex.StackTrace);
                }
            }
            catch (Exception ex)
            {
                logger.Error("Error in SaveToMsmq:" + ex.StackTrace);
            }
        }

        public void CloseSocket(StateObject state)
        {
            try
            {
                if (connectedImei.ContainsKey(state.imei))
                {
                    var sotemp = connectedImei[state.imei];
                    if (sotemp.ip == state.ip)
                    {
                        connectedImei.TryRemove(state.imei, out sotemp );
                        logger.Info("CloseSocket remove {0} {1} from connection list", state.imei, state.ip);
                    }
                }
                if (state.timer != null)
                {
                    state.timer.Dispose();
                    state.timer = null;
                }
                if (state.codec12Requestor != null && state.codec12Requestor.timer != null)
                {
                    state.codec12Requestor.timer.Dispose();
                    state.codec12Requestor.timer = null;
                }
                
                if (state.tcpClient != null)
                {
                    logger.Info($"Task {tid} CloseSocket " + state.ip + " " + state.imei);
                    state.tcpClient.GetStream().Close();
                    state.tcpClient.Close();
                    state.tcpClient = null;
                }
            }
            catch (Exception ex)
            {
                logger.Info($"Task {tid} Error on CloseSocket {state.ip} {ex}");
            }
        }
        public void OnTimer(Object state) {
            try
            {
                if (state != null)
                {
                    StateObject s = (StateObject)state;
                    logger.Info("Timeout on connection" + s.ip + " " + s.imei + " Count = " + connectedImei.Count);
                    if ( connectedImei.ContainsKey(s.imei))
                    {
                        var sotemp = connectedImei[s.imei];
                        if (sotemp.ip ==  s.ip)
                        {
                            CloseSocket(s);
                        }
                        else
                        {
                            logger.Info("current connection has different ip timout is on {0}, current ip {1}", s.ip, sotemp.ip);
                        }
                    }
                    else
                    {
                        //logger.Info("imei {0} not in connection list");
                    }
                }
            }
            catch (Exception ex)
            {
                logger.Error("Task {tid} Error in timer callback {0}", ex.Message);
            }
        }
              
        public void OnTimer2(Object state)
        {
            try
            {
                if (state != null)
                {
                    StateObject s = (StateObject)state;
                    logger.Info("Task {tid} Timout codec12 receive " + s.ip + " " + s.imei + " Count = " + connectedImei.Count);
                    if (s.timer != null)
                    {
                        s.timer.Dispose();
                        s.timer = null;
                    }
                    if (s.codec12Requestor != null)
                    {
                        var e = new Codec12Error("no reply from gps unit after 30 seconds");
                        var ej = JsonConvert.SerializeObject(e);
                        (((StateObject)state).tcpClient.GetStream()).Write(System.Text.Encoding.UTF8.GetBytes(ej), 0, ej.Length);
                    }
                }
            }
            catch (Exception ex)
            {
                logger.Error("Task {tid} Error in timer2 callback {0}", ex.Message);
            }
        }

        byte[] BuildMsg(string command)
        {
            try
            {

                //string hex = "00000000000000180C0105000000102347455420444154414F524445520D0A0100004990";
                Crc16 crc16 = new Crc16();
                byte[] msg = new byte[4 + 4 + 1 + 1 + 1 + 4 + command.Length + 1 + 4];
                Byte[] lenM = BitConverter.GetBytes(command.Length + 8);                 // Packet Length
                Array.Reverse(lenM);
                lenM.CopyTo(msg, 4);
                msg[8] = 12;                                                             // Codec ID
                msg[9] = 1;                                                              // Number of commands
                msg[10] = 5;                                                             // Command Type
                Byte[] lenC = BitConverter.GetBytes(command.Length);                   // Command  Length
                Array.Reverse(lenC);
                lenC.CopyTo(msg, 11);
                Encoding.ASCII.GetBytes(command).CopyTo(msg, 15);                       // Command
                msg[command.Length + 15] = 1;                                          // Number of commands
                byte[] crc = crc16.ComputeChecksumBytes(msg, 8, command.Length + 16);   // CRC
                Array.Reverse(crc);
                crc.CopyTo(msg, command.Length + 18);
                return (msg);
            }
            catch (Exception ex)
            {
                logger.Error("Error on BuildMessage " + ex.StackTrace);
            }
            return null;
        }
        public void CloseDB()
        {
            try
            {
                if (mainConnection != null &&
                    mainConnection._connection != null &&
                    mainConnection._connection.State == ConnectionState.Open)
                {
                    mainConnection._connection.Close();
                    mainConnection._connection = null;
                }
                /*
                if (dataConnection != null &&
                    dataConnection._connection != null &&
                    dataConnection._connection.State == ConnectionState.Open)
                {
                    dataConnection._connection.Close();
                    dataConnection._connection = null;
                }
                */
            }
            catch (Exception ex)
            {
                logger.Error($" Tasl {tid} Closedb error {ex}");
            }
        }
        public GpsBoxBO SelectGpsByImei2(string imei)
        {
            GpsBoxBO gpsBoxBO = null;
            var connectionString = Properties.Settings.Default.DBConnection;
            using (System.Data.SqlClient.SqlConnection conn = new SqlConnection(connectionString))
            {
                using (SqlCommand command = new SqlCommand("select * from tgps with (NOLOCK) where imei = @IMEI", conn))
                {
                    command.Parameters.Add(new SqlParameter("@IMEI", imei));
                    try
                    {
                        conn.Open();
                        using (SqlDataReader reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                var gpsBoxDBO = new GpsBoxDBO();
                                if (reader.Read())
                                {
                                    gpsBoxBO = gpsBoxDBO.MakeGps(reader, conn);
                                }
                            }
                            else
                            {
                                logger.Error($"Task {tid} Gps with imei {imei} not found in database");
                            }
                        }
                    }
                    catch (Exception ex)
                    {

                        gpsBoxBO = null;
                        logger.Error($"Task {tid} Gps with imei {imei} Error on read {ex}");
                    }
                    finally
                    {
                        if (conn.State == ConnectionState.Open)
                        {
                            conn.Close();
                        }
                    }
                }
            }
            return gpsBoxBO;
        }
        public int Canbus(string boxtype)
        {
            string FMB = Properties.Settings.Default.FMB;
            string[] fmbs = FMB.Split(',');
            int type = 0;
            if (boxtype == "FM1100C")
            {
                type = 1;
            }
            else if (boxtype == "FM-CAN")
            {
                type = 2;
            }
            else if (fmbs.Contains(boxtype))
            {
                type = 3;
            }
            return type;
        }
    }


    
    public class StateObject
    {
        // type of connection 0 = old 1 is fm2200
        public string ip = "";
        public TcpClient tcpClient = null;
        public string imei = "";
        public string gpsSerial = "";
        public string boxTypeName = "";
        public string ibutton = "";
        public Timer timer;
        public int numreads = 0;
        public StateObject codec12Requestor = null;
        public string c12reply = "";
    }

}
