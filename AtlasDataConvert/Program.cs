using AtlasDataConvert.Services;
using System;
using System.Collections.Generic;
using System.Data.OleDb;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AtlasDataConvert
{
    class Program
    {
        static void Main(string[] args)
        {

            // ProcessDataConversion.ProcessActivityAllocation("");
            ProcessDataConversion.ProcessPODetail("");

            ProcessDataConversion.ProcessXml(@"C:\Development\ProjectAtlas\DataConversion\CapsysTemplates\tc_ActivityAllocation.xml");

            foreach (string fileName in Directory.GetFiles(AppDomain.CurrentDomain.BaseDirectory + @"\Input"))
            {
                Console.WriteLine("Processing::{0}", fileName);
                ProcessDataConversion.ProcessXml(fileName);
            }
            Console.ReadKey();
        }
    }
}
