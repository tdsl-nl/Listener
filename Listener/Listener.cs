using System;
using System.ServiceProcess;
using NLog;

namespace Listener
{

    public partial class Listener : ServiceBase
    {
        private static Logger logger = LogManager.GetCurrentClassLogger();
        public static AsyncServer asyncServer;
        static void Main(string[] args)
        {
            
            AppDomain currentDomain = AppDomain.CurrentDomain;
            currentDomain.UnhandledException += new UnhandledExceptionEventHandler(MyHandler);
            Listener service = new Listener();

            if (Environment.UserInteractive)
            {
                service.OnStart(args);
                System.Threading.Thread.Sleep(System.Threading.Timeout.Infinite);
                service.OnStop();
            }
            else
            {
                ServiceBase.Run(service);
            }

        }
        static void MyHandler(object sender, UnhandledExceptionEventArgs args)
        {
            Exception e = (Exception)args.ExceptionObject;
            logger.Error("MyHandler caught : {0} \r\n{1}" + e.Message, e.StackTrace);
        }

        public Listener()
        {
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {
            int port = Properties.Settings.Default.Port;
            Listener.asyncServer = new AsyncServer(port);
            logger.Info("Start Listener on port #{0}", port);
            Listener.asyncServer.Start();
        }

        protected override void OnStop()
        {
            logger.Info("Stop Listener");
            // TODO: Add code here to perform any tear-down
            //necessary to stop your service.

        }
    }
}
