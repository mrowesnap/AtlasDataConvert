using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AtlasDataConvert.Services
{
    public static class LogService
    {
        static string LogFileName = AppDomain.CurrentDomain.BaseDirectory + @"\DataConverstionLog.txt";

        public static void LogMessage(string message)
        {
            File.AppendAllText(LogFileName, message);
        }
    }
}
