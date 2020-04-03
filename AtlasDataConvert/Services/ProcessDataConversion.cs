using AtlasDataConvert.Enumerations;
using AtlasDataConvert.Models;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using AtlasDataConvert.Extensions;
using GenericParsing;

namespace AtlasDataConvert.Services
{
    public static class ProcessDataConversion
    {
        public static void ProcessXml(string xmlFile)
        {
            DatabaseTableModel inputTable = new DatabaseTableModel();

            inputTable = ProcessDataFilesService.LoadTableModel(xmlFile);

            switch (inputTable.SourceDatabaseType)
            {
                case DatabaseType.CSV:
                    ProcessCSVSourceData(inputTable);
                    break;
                case Enumerations.DatabaseType.SQLServer:
                    ProcessSQLSourceData(inputTable);
                    break;
                case Enumerations.DatabaseType.FoxPro:
                    ProcessFoxProData(inputTable);
                    break;
                default:
                    break;
            }

        }

        private static void ProcessCSVSourceData(DatabaseTableModel inputTable)
        {
            DataTable convertedCsv = null;
            using (GenericParserAdapter parser = new GenericParserAdapter())
            {
                parser.SetDataSource(inputTable.SourceConnectionString);

                parser.ColumnDelimiter = ',';
                parser.FirstRowHasHeader = true;
                parser.TextQualifier = '\"';
                convertedCsv = parser.GetDataTable();
            }
            ProcessDataTable(inputTable, convertedCsv);
        }

        
        private static void ProcessSQLSourceData(DatabaseTableModel mappingTable)
        {

            StringBuilder commandText = new StringBuilder();
            int counter = 1;
            if (mappingTable.SourceCommandType == Enumerations.CommandType.Table)
            {
                commandText.Append("SELECT ");
                foreach (DatabaseField field in mappingTable.Fields)
                {
                    commandText.Append(field.SourceName);
                    if (counter < mappingTable.Fields.Count)
                    {
                        counter++;
                        commandText.Append(",");
                    }
                }
                commandText.Append(" FROM ");
                commandText.Append(mappingTable.SourceName);
            }
            else
            {
                commandText.Append(mappingTable.SourceName);
            }
            using (SqlConnection cn = new SqlConnection(mappingTable.SourceConnectionString))
            {
                cn.Open();
                using (SqlCommand cmd = cn.CreateCommand())
                {
                    cmd.CommandText = commandText.ToString();
                    using (IDataReader reader = cmd.ExecuteReader())
                    {
                        ProcessOutputFromDataReader(mappingTable, reader);
                    }

                }
            }
        }

        private static void ProcessDataTable(DatabaseTableModel mappingTable, DataTable table)
        {
            StringBuilder outputCsv = new StringBuilder();
            string dataValue = string.Empty;
            string validationMessage = string.Empty;
            int counter = 1;
            int lineNumber = 0;
            string finalDataValue = string.Empty;
            string sourceField = string.Empty;
            List<string> sourceFields = null;
            int subFieldCount = 0;
            foreach (DatabaseField field in mappingTable.Fields)
            {
                outputCsv.Append(field.DestinationName);
                if (counter < mappingTable.Fields.Count)
                {
                    counter++;
                    outputCsv.Append(",");
                }
            }
            outputCsv.Append(Environment.NewLine);

            foreach(DataRow row in table.Rows)
            {
                lineNumber++;
                int dataLength = 0;

                counter = 1;
                foreach (DatabaseField field in mappingTable.Fields)
                {
                    dataValue = string.Empty;
                    sourceField = field.SourceName;
                    sourceFields = new List<string>();
                    if (sourceField.Contains(";"))
                    {
                        foreach (string f in sourceField.Split(';'))
                        {
                            sourceFields.Add(f);
                        }
                    }
                    else
                    {
                        sourceFields = new List<string>();
                        sourceFields.Add(sourceField);
                    }
                    finalDataValue = string.Empty;
                    subFieldCount = 0;
                    foreach (string fieldName in sourceFields)
                    {
                        dataValue = GetDataTableValue(fieldName, row).ToString();
                        dataValue = FormatDataValue(dataValue, field.FieldDataType, field.DataLength,false, out validationMessage);
                        if (subFieldCount > 0)
                        {
                            finalDataValue += ";";
                        }
                        subFieldCount++;
                        finalDataValue += dataValue;
                    }
                    finalDataValue = FormatDataValue(finalDataValue, field.FieldDataType, field.DataLength,true, out validationMessage);

                    if (!string.IsNullOrEmpty(validationMessage))
                    {
                        LogService.LogMessage(string.Format("{0}, Line::{1} of {2} - Column {3}", validationMessage, lineNumber.ToString(), mappingTable.SourceName, field.SourceName));
                    }
                    outputCsv.Append(finalDataValue);
                    if (counter < mappingTable.Fields.Count)
                    {
                        counter++;
                        outputCsv.Append(",");
                    }
                }
                Console.WriteLine("Processing Row::{0}", lineNumber);
                outputCsv.Append(Environment.NewLine);
            }

            if (File.Exists(mappingTable.DestinationConnectionString))
            {
                File.Delete(mappingTable.DestinationConnectionString);
            }

            File.AppendAllText(mappingTable.DestinationConnectionString, outputCsv.ToString());
        }

        private static string FormatDataValue(string input, DataType fieldType, string length, bool formatString, out string validationMessage)
        {
            string dataValue = string.Empty;
            int dataLength = 0;
            validationMessage = string.Empty;

            switch (fieldType)
            {
                case DataType.Text:
                    if (!string.IsNullOrEmpty(length))
                    {
                        dataLength = Convert.ToInt32(length);
                        if (input.Length > dataLength)
                        {
                            dataValue = input.Substring(0, dataLength);
                            validationMessage = "String Truncated";
                        }
                    }
                    if (formatString)
                    {
                        dataValue = string.Format("\"{0}\"", input.Replace("NULL","").Replace("\"",""));
                    }
                    else
                    {
                        dataValue = input.Replace("NULL", "").Replace("\"", "");
                    }
                    break;
                case DataType.Phone:
                    dataValue = GetAllDigits(input);
                    break;
                case DataType.Boolean:
                    if (!string.IsNullOrEmpty(input))
                    {
                        if (input.ToLower() == "t" || input == "y"
                                || input.ToLower() == "true" || input.ToLower() == "yes"
                                || input.ToLower() == "1")
                        {
                            dataValue = "1";
                        }
                        else
                        {
                            dataValue = "0";
                        }
                    }
                    break;
                case DataType.Date:
                    DateTime dt = Convert.ToDateTime(dataValue);
                    dataValue = dt.ToString("yyyy-MM-dd");
                    break;
                default:
                    dataValue = input;
                    break;
            }
            return dataValue;
        }

        private static string GetAllDigits(string dataValue)
        {
            return new string(dataValue.Where(c => char.IsDigit(c)).ToArray());
        }

        private static void ProcessOutputFromDataReader(DatabaseTableModel mappingTable, IDataReader reader)
        {
            StringBuilder outputCsv = new StringBuilder();
            string dataValue = string.Empty;
            string validationMessage = string.Empty;

            string finalDataValue = string.Empty;
            string sourceField = string.Empty;
            List<string> sourceFields = null;
            int subFieldCount = 0;


            int counter = 1;
            int lineNumber = 0;
            foreach (DatabaseField field in mappingTable.Fields)
            {
                outputCsv.Append(field.DestinationName);
                if (counter < mappingTable.Fields.Count)
                {
                    counter++;
                    outputCsv.Append(",");
                }
            }
            outputCsv.Append(Environment.NewLine);

            while (reader.Read())
            {
                lineNumber++;
                counter = 1;
                foreach (DatabaseField field in mappingTable.Fields)
                {
                    dataValue = string.Empty;
                    sourceField = field.SourceName;
                    sourceFields = new List<string>();
                    if (sourceField.Contains(";"))
                    {
                        foreach (string f in sourceField.Split(';'))
                        {
                            sourceFields.Add(f);
                        }
                    }
                    else
                    {
                        sourceFields = new List<string>();
                        sourceFields.Add(sourceField);
                    }
                    finalDataValue = string.Empty;
                    subFieldCount = 0;
                    foreach (string fieldName in sourceFields)
                    {
                        dataValue = GetDataReaderValue(fieldName, reader).ToString();
                        dataValue = FormatDataValue(dataValue, field.FieldDataType, field.DataLength, false, out validationMessage);
                        if (subFieldCount > 0)
                        {
                            finalDataValue += "_";
                            
                        }
                        subFieldCount++;
                        finalDataValue += dataValue;
                    }
                    finalDataValue = FormatDataValue(finalDataValue, field.FieldDataType, field.DataLength, true, out validationMessage);

                    if (!string.IsNullOrEmpty(validationMessage))
                    {
                        LogService.LogMessage(string.Format("{0}, Line::{1} of {2} - Column {3}", validationMessage, lineNumber.ToString(), mappingTable.SourceName, field.SourceName));
                    }
                    outputCsv.Append(finalDataValue);
                    if (counter < mappingTable.Fields.Count)
                    {
                        counter++;
                        outputCsv.Append(",");
                    }
                }
                Console.WriteLine("Processing Row::{0}", lineNumber);
                outputCsv.Append(Environment.NewLine);
            }

            if (File.Exists(mappingTable.DestinationConnectionString))
            {
                File.Delete(mappingTable.DestinationConnectionString);
            }

            File.AppendAllText(mappingTable.DestinationConnectionString, outputCsv.ToString());
        }

        public static object GetDataReaderValue(string fieldName, IDataReader dr)
        {
            object val = null;
            int ordinal = 0;
            if (dr.HasColumn(fieldName))
            {
                ordinal = dr.GetOrdinal(fieldName);
                try
                {
                    val = dr.GetValue(ordinal);
                }
                catch (System.InvalidOperationException ex) { }
                if (val != null && val != DBNull.Value)
                {
                    return val;
                }
                else
                {
                    return "";
                }
            }
            else
            {
                return "";
            }
        }

        public static object GetDataTableValue(string fieldName, DataRow dr)
        {
            object val = null;

            if (dr.Table.Columns.Contains(fieldName))
            {
                val = dr[fieldName];
                if (val != null && val != DBNull.Value)
                {
                    return val;
                }
                else
                {
                    return "";
                }
            }
            else
            {
                return "";
            }
        }

        private static void ProcessFoxProData(DatabaseTableModel mappingTable)
        {
            StringBuilder commandText = new StringBuilder();

            int counter = 1;
            if (mappingTable.SourceCommandType == Enumerations.CommandType.Table)
            {
                commandText.Append("SELECT ");
                foreach (DatabaseField field in mappingTable.Fields)
                {
                    counter++;

                    if (!string.IsNullOrEmpty(field.SourceName))
                    {
                        commandText.Append(field.SourceName);
                        if (counter < mappingTable.Fields.Count)
                        {
                            commandText.Append(",");
                        }
                    }
                  

                }
                if (commandText.ToString().EndsWith(","))
                {
                    commandText.Remove(commandText.ToString().Length-1,1);
                }
                commandText.Append(" FROM ");
                commandText.Append(mappingTable.SourceName);
            }
            else
            {
                commandText.Append(mappingTable.SourceName);
            }
            Console.WriteLine(commandText.ToString());
            Console.WriteLine("Total Counted::{0} Total Fields::{1}", counter.ToString(), mappingTable.Fields.Count.ToString());
            using (OleDbConnection cn = new OleDbConnection(mappingTable.SourceConnectionString))
            {
                cn.Open();
                using (OleDbCommand cmd = cn.CreateCommand())
                {
                    cmd.CommandText = commandText.ToString();
                    using (IDataReader reader = cmd.ExecuteReader())
                    {
                        ProcessOutputFromDataReader(mappingTable, reader);
                    }

                }
            }       
        }
        public static void ProcessActivityAllocation(string tableNameString)
        {
            StringBuilder commandText = new StringBuilder();
            StringBuilder outputTextFile = new StringBuilder();
            commandText.Append("SELECT * FROM act WHERE (ATYP <> '' OR ATYP <> 'X') and NOT AGENCALC");
            Console.WriteLine(commandText.ToString());
            outputTextFile.Append("Group__c, Name, General_Accounting_Unit__c, Allocation_Percent__c");
            outputTextFile.Append(Environment.NewLine);
            using (OleDbConnection cn = new OleDbConnection(@"Provider=vfpoledb;Data Source=C:\Development\ProjectAtlas\DataConversion\Timecard files\act.dbf;Collating Sequence=machine;"))
            {
                cn.Open();
                using (OleDbCommand cmd = cn.CreateCommand())
                {
                    cmd.CommandText = commandText.ToString();
                    using (IDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            for (int i = 1; i <= 16; i++)
                            {
                                if (!string.IsNullOrEmpty(GetDataReaderValue("aacc" + i.ToString(), reader).ToString().Trim()))
                                {
                                    outputTextFile.Append(GetDataReaderValue("agrp", reader).ToString().Trim());
                                    outputTextFile.Append(",\"");
                                    outputTextFile.Append(GetDataReaderValue("aact", reader).ToString().Trim());
                                    outputTextFile.Append("\",\"");
                                    outputTextFile.Append(GetDataReaderValue("aacc" + i.ToString(), reader).ToString().Trim()+"6000");
                                    outputTextFile.Append("\",");
                                    outputTextFile.Append(GetDataReaderValue("apc" + i.ToString(), reader).ToString().Trim());
                                    outputTextFile.Append(Environment.NewLine);
                                }                                
                            }
                        }
                    }
                    File.AppendAllText(@"C:\Development\ProjectAtlas\DataConversion\Timecard files\tc_activity_allocation.csv", outputTextFile.ToString());
                }
            }
        }

        public static void ProcessPODetail(string tableNameString)
        {
            StringBuilder commandText = new StringBuilder();
            StringBuilder outputTextFile = new StringBuilder();
            commandText.Append("SELECT * FROM pod");
            Console.WriteLine(commandText.ToString());
            outputTextFile.Append("Account__c, Amount__c, Description__c, General_Accounting_Unit__c,Open_Date__c,PO_Line_Number__c,Purchase_Order__c");
            outputTextFile.Append(Environment.NewLine);
            int k = 0;
            using (OleDbConnection cn = new OleDbConnection(@"Provider=vfpoledb;Data Source=C:\Development\ProjectAtlas\DataConversion\POFiles\pod.dbf;Collating Sequence=machine;"))
            {
                cn.Open();
                using (OleDbCommand cmd = cn.CreateCommand())
                {
                    cmd.CommandText = commandText.ToString();
                    using (IDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            Console.WriteLine("Processing Line::" + k.ToString());
                            k++;
                            for (int i = 1; i <= 6; i++)
                            {
                                if (!string.IsNullOrEmpty(GetDataReaderValue("PACC" + i.ToString(), reader).ToString().Trim()))
                                {
                                    outputTextFile.Append(GetDataReaderValue("PACC" + i.ToString(), reader).ToString().Trim());
                                    outputTextFile.Append(",");
                                    outputTextFile.Append(GetDataReaderValue("PAMT" + i.ToString(), reader).ToString().Trim());
                                    outputTextFile.Append(",\"");
                                    outputTextFile.Append(GetDataReaderValue("PDESC", reader).ToString().Trim());
                                    outputTextFile.Append("\",");
                                    outputTextFile.Append(GetDataReaderValue("PACC" + i.ToString(), reader).ToString().Trim());
                                    outputTextFile.Append(",");
                                    outputTextFile.Append(Convert.ToDateTime(GetDataReaderValue("PDATORD", reader)).ToString("yyyy-MM-dd"));
                                    outputTextFile.Append(",");
                                    outputTextFile.Append(i.ToString());
                                    outputTextFile.Append(",");
                                    outputTextFile.Append(GetDataReaderValue("PNUM", reader).ToString());
                                    outputTextFile.Append(Environment.NewLine);
                                }
                            }
                        }
                    }
                    File.AppendAllText(@"C:\Development\ProjectAtlas\DataConversion\POFiles\purchase_order_line.csv", outputTextFile.ToString());
                }
            }
        }

    }
}
