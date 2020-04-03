using AtlasDataConvert.Enumerations;
using AtlasDataConvert.Models;
using AtlasDataConvert.Services;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CreateXml
{
    class Program
    {
        static void Main(string[] args)
        {
            DatabaseTableModel table = new DatabaseTableModel();
            string[] splitLine;
            string required;
            DatabaseField field = new DatabaseField();
            string directory = @"C:\Development\ProjectAtlas\DataConversion\CapsysTemplates";// AppDomain.CurrentDomain.BaseDirectory;
            //string directory = AppDomain.CurrentDomain.BaseDirectory;
            foreach (string fileName in Directory.GetFiles(directory + @"\Input"))
            {
                Console.WriteLine("Processing::{0}", fileName);
                table = new DatabaseTableModel();
                table.DestinationDatabaseType = DatabaseType.CSV;
                table.SourceDatabaseType = DatabaseType.FoxPro;
                table.SourceCommandType = CommandType.Table;
                table.DestinationConnectionString = "";
                table.DestinationName = Path.GetFileNameWithoutExtension(fileName);
                table.SourceName = Path.GetFileNameWithoutExtension(fileName);
                table.SourceConnectionString = table.SourceName;

                table.Fields = new List<DatabaseField>();
                foreach (string line in File.ReadAllLines(fileName))
                {

                    field = new DatabaseField();
                    splitLine = line.Split(',');
                    field.DataLength = splitLine[5];
                    field.DestinationName = splitLine[2];
                    field.FieldDataType = GetDataTypeFromString(splitLine[0]);
                    field.SourceName = splitLine[3];
                    required = splitLine[12].ToUpper();
                    field.IsRequired = required == "TRUE" ? true : false;
                    if (!splitLine[0].Contains("Formula") && !splitLine[0].Contains("Address"))
                    {
                        table.Fields.Add(field);
                    }

                }
                File.Delete(fileName.Replace("Input", "Output").Replace(".csv", ".xml"));
                ProcessDataFilesService.GenerateModelXml(table, fileName.Replace("Input", "Output").Replace(".csv",".xml"));
                
            }
            Console.ReadKey();
        }

        private static DataType GetDataTypeFromString(string v)
        {
            DataType dt = DataType.Text;
            switch (v.ToLower())
            {
                case string a when a.Contains("number"):
                case string b when b.Contains("currency"):
                case string c when c.Contains("percent"):
                    dt = DataType.Number;
                    break;
                default:
                    dt = DataType.Text;
                    break;
            }
            return dt;
        }
    }
}
