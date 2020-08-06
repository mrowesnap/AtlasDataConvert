using AtlasDataConvert.Services;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.OleDb;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace AtlasDataConvert
{
    class Program
    {
        static void Main(string[] args)
        {
            List<Thread> processData = new List<Thread>();
            //processData.Add(new Thread(ProcessDataConversion.ProcessUser));
            //processData.Add(new Thread(ProcessDataConversion.ProcessService));
            //processData.Add(new Thread(ProcessDataConversion.ProcessTimesheetGroupMember));
            //processData.Add(new Thread(ProcessDataConversion.ProcessPODetail));
            //processData.Add(new Thread(ProcessDataConversion.ProcessProjectAccount));

            foreach (string directoryName in Directory.GetDirectories(ConfigurationManager.AppSettings["crystalReportsPath"]))
            {
                foreach (string reportName in Directory.GetFiles(directoryName))
                {
                  //  processData.Add(new Thread(() => ProcessDataConversion.ProcessCrystalReport(reportName)));
                }

            }
            foreach (Thread t in processData)
            {
                t.Start();
            }

            ProcessDataConversion.ProcessActivityAllocation();
            //ProcessDataConversion.ProcessUser("");
            //ProcessDataConversion.ProcessRent("");
            ProcessDataConversion.ProcessService();
            //  ProcessDataConversion.ProcessProjectAccount();
            //Console.WriteLine("Complete");
            Console.ReadKey();
        }

    }
}
