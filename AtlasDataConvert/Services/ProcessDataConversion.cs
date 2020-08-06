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
using System.Configuration;
using CrystalDecisions.CrystalReports.Engine;
using CrystalDecisions.Shared;

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

            foreach (DataRow row in table.Rows)
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
                        dataValue = FormatDataValue(dataValue, field.FieldDataType, field.DataLength, false, out validationMessage);
                        if (subFieldCount > 0)
                        {
                            finalDataValue += ";";
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
                        dataValue = string.Format("\"{0}\"", input.Replace("NULL", "").Replace("\"", ""));
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
                    commandText.Remove(commandText.ToString().Length - 1, 1);
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

        public static void ProcessActivityAllocation()
        {
            StringBuilder commandText = new StringBuilder();
            StringBuilder outputTextFile = new StringBuilder();
            commandText.Append("SELECT * FROM act WHERE (ATYP <> '' OR ATYP <> 'X') and AGENCALC > 0");
            Console.WriteLine(commandText.ToString());
            outputTextFile.Append("Group__c, Name, General_Accounting_Unit__c, Allocation_Percent__c");
            outputTextFile.Append(Environment.NewLine);
            using (SqlConnection cn = new SqlConnection(ConfigurationManager.AppSettings["connectionString"]))
            {
                cn.Open();
                using (SqlCommand cmd = cn.CreateCommand())
                {
                    cmd.CommandText = commandText.ToString();
                    using (IDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            for (int i = 1; i <= 16; i++)
                            {
                                if (!string.IsNullOrEmpty(GetDataReaderValue("aacc" + i.ToString(), reader).ToString().Trim())
                                    && (
                                        !string.IsNullOrEmpty(GetDataReaderValue("apc" + i.ToString(), reader).ToString().Trim()) 
                                        && Convert.ToDecimal(GetDataReaderValue("apc" + i.ToString(), reader).ToString().Trim()) > 0
                                       )
                                   )
                                {
                                    outputTextFile.Append(GetDataReaderValue("agrp", reader).ToString().Trim());
                                    outputTextFile.Append(",\"");
                                    outputTextFile.Append(GetDataReaderValue("aact", reader).ToString().Trim());
                                    outputTextFile.Append("\",\"");
                                    outputTextFile.Append(GetDataReaderValue("aacc" + i.ToString(), reader).ToString().Trim() + "6000");
                                    outputTextFile.Append("\",");
                                    outputTextFile.Append(GetDataReaderValue("apc" + i.ToString(), reader).ToString().Trim());
                                    outputTextFile.Append(Environment.NewLine);
                                }
                            }
                        }
                    }
                    File.AppendAllText(ConfigurationManager.AppSettings["outputDirectory"] + @"tc_activity_allocation.csv", outputTextFile.ToString());
                }
            }
        }

        public static void ProcessPODetail()
        {
            StringBuilder commandText = new StringBuilder();
            StringBuilder outputTextFile = new StringBuilder();
            commandText.Append("SELECT * FROM POD WHERE PSTAGE = 15.00 AND DATEDIFF(DAY, pod.PDATORD, '1/1/2013') <= 0 AND ISNUMERIC(pod.PREFPRJNUM) = 0 AND pod.PDTL NOT IN('M', 'W') and pVoid is null");
            Console.WriteLine(commandText.ToString());
            string completeDate;
            DateTime compDate;
            outputTextFile.Append("Amount__c, General_Accounting_Unit__c,Complete_Date__c,PO_Line_Number__c,Purchase_Order__c");
            //where year(PDATORD) is 2010 and later, pVoid is null, pstage=15
            outputTextFile.Append(Environment.NewLine);
            int k = 0;
            using (SqlConnection cn = new SqlConnection(ConfigurationManager.AppSettings["connectionString"]))
            {
                //ignore where PREFPRJNUM is not null
                cn.Open();
                using (SqlCommand cmd = cn.CreateCommand())
                {
                    cmd.CommandText = commandText.ToString();
                    using (IDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            object dateValue = GetDataReaderValue("PDATINV", reader);
                            if (dateValue != null && dateValue != DBNull.Value)
                            {
                                if (DateTime.TryParse(GetDataReaderValue("PDATINV", reader).ToString(), out compDate))
                                {
                                    completeDate = compDate.ToString("yyyy-MM-dd");
                                }
                                else
                                {
                                    completeDate = "";
                                }
                            }
                            else
                            {
                                completeDate = "";
                            }
                            Console.WriteLine("Processing Line::" + k.ToString());
                            k++;

                            for (int i = 1; i <= 6; i++)
                            {
                                if (!string.IsNullOrEmpty(GetDataReaderValue("PACC" + i.ToString(), reader).ToString().Trim()))
                                {
                                    
                                    outputTextFile.Append(GetDataReaderValue("PAMT" + i.ToString(), reader).ToString().Trim());
                                    outputTextFile.Append(",\"");
                                    outputTextFile.Append(GetGAUAccountNumberForPO(GetDataReaderValue("PACC" + i.ToString(), reader).ToString().Trim()));
                                    outputTextFile.Append("\",");
                                    outputTextFile.Append(completeDate);
                                    outputTextFile.Append(",");
                                    outputTextFile.Append(i.ToString());
                                    outputTextFile.Append(",");
                                    outputTextFile.Append(GetDataReaderValue("PNUM", reader).ToString());
                                    outputTextFile.Append(Environment.NewLine);
                                }
                            }

                        }
                    }
                    File.AppendAllText(ConfigurationManager.AppSettings["outputDirectory"] + @"purchase_order_line.csv", outputTextFile.ToString());
                }
            }
        }

        public static void ProcessProjectAccount()
        {
            StringBuilder commandText = new StringBuilder();
            StringBuilder outputTextFile = new StringBuilder();
            commandText.Append("SELECT DISTINCT 'Occupant' as Account_Type__c, cli.cidnum as Account__c, ");
            commandText.Append("'EHR' + rtRIM(LTRIM(CAST(DPNUM AS VARCHAR(200)))) ");


            commandText.Append("AS Capsys_ProjectID__C ");
            commandText.Append("FROM  cli INNER JOIN mem ON(cli.CCTR = mem.MCTR AND cli.CSSN1 = mem.MSSN1) ");
            commandText.Append("INNER JOIN mdt ON(cli.CCTR = mdt.DCTR AND cli.CSSN1 = mdt.DSSN1) ");
            commandText.Append("WHERE mem.MMHROWN = 1 AND MEM.MSSN = mem.mssn1 and DSRVNUM = 1 ");

            commandText.Append("UNION ALL ");

            commandText.Append("SELECT DISTINCT 'Owner' as Account_Type__c, ");
            commandText.Append("cli.cidnum as Account__c, ");
            commandText.Append("PRJ.PNUM AS Capsys_ProjectID__C ");
            commandText.Append("FROM PRJ ");

            commandText.Append("    INNER JOIN WCL ON PRJ.PNUM = WCL.CPNUM ");

            commandText.Append("LEFT OUTER JOIN MEM ON WCL.CSSN1 = MEM.MSSN ");

            commandText.Append("LEFT OUTER JOIN CLI  ON(cli.CCTR = mem.MCTR AND cli.CSSN1 = mem.MSSN1) ");
            commandText.Append("WHERE mem.MMHROWN = 1 AND MEM.MSSN = mem.mssn1 ");
            commandText.Append("and PRJ.PDATCERT >= '1/1/2013' ");
            commandText.Append("and prj.PSTAT = 'C'");

            Console.WriteLine(commandText.ToString());
            
            outputTextFile.Append("Account_Type__c, Account__c, Capsys_ProjectID__C");
            //where year(PDATORD) is 2010 and later, pVoid is null, pstage=15
            outputTextFile.Append(Environment.NewLine);
            int k = 0;
            using (SqlConnection cn = new SqlConnection(ConfigurationManager.AppSettings["connectionString"]))
            {
                //ignore where PREFPRJNUM is not null
                cn.Open();
                using (SqlCommand cmd = cn.CreateCommand())
                {
                    cmd.CommandText = commandText.ToString();
                    using (IDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            Console.WriteLine("Processing Line::" + k.ToString());
                            k++;

                            outputTextFile.Append("\"");
                            outputTextFile.Append(GetDataReaderValue("Account_Type__c", reader).ToString().Trim());
                            outputTextFile.Append("\",\"");
                            outputTextFile.Append(GetDataReaderValue("Account__c", reader).ToString().Trim());
                            outputTextFile.Append("\",\"");
                            outputTextFile.Append(GetDataReaderValue("Capsys_ProjectID__C", reader).ToString().Trim());
                            outputTextFile.Append("\"");
                            outputTextFile.Append(Environment.NewLine);
                        }
                    }
                    File.AppendAllText(ConfigurationManager.AppSettings["outputDirectory"] + @"project_account.csv", outputTextFile.ToString());
                }
            }

            //using (SqlConnection cn = new SqlConnection(ConfigurationManager.AppSettings["connectionString"]))
            //{
            //    //ignore where PREFPRJNUM is not null
            //    cn.Open();
            //    using (SqlCommand cmd = cn.CreateCommand())
            //    {
            //        cmd.CommandText = "SELECT 	case when len(rtrim(ltrim(PLNUM))) > 0 THEN 'Landlord' ELSE 'Occupant' END as Account_Type__C,	prj.PNUM AS Capsys_ProjectID__C,	case when NOT LAN.LFEVEN is null then LAN.LFEVEN else wcl.CIDNUM end as Account__C FRom prj 	INNER JOIN WCL on		(prj.PNUM = wcl.CPNUM)	LEFT OUTER JOIN LAN on		(prj.PLNUM = lan.LNUM)	WHERE datediff(d, '1/1/2013', prj.pdatcert) >=0 and		(LAN.LFEVEN is null OR LAN.LFEVEN >0)";
            //        using (IDataReader reader = cmd.ExecuteReader())
            //        {
            //            while (reader.Read())
            //            {
            //                Console.WriteLine("Processing Line::" + k.ToString());
            //                k++;

            //                outputTextFile.Append("\"");
            //                outputTextFile.Append(GetDataReaderValue("Account_Type__c", reader).ToString().Trim());
            //                outputTextFile.Append("\",\"");
            //                outputTextFile.Append(GetDataReaderValue("Account__c", reader).ToString().Trim());
            //                outputTextFile.Append("\",\"");
            //                outputTextFile.Append(GetDataReaderValue("Capsys_ProjectID__C", reader).ToString().Trim());
            //                outputTextFile.Append("\"");
            //                outputTextFile.Append(Environment.NewLine);
            //            }
            //        }
            //        File.AppendAllText(ConfigurationManager.AppSettings["outputDirectory"] + @"project_account.csv", outputTextFile.ToString());
            //    }
            //}
        }

        private static string GetFundNumberForPO(string fund)
        {
            string result = "Not Found";

            switch (fund)
            {
                case "ARMORY   7050":
                case "ARMGAS   7240":
                case "ARMORY   7040":
                case "ARMORY   7060":
                case "ARMTOOLS 7230":
                case "DC       7040":
                case "DC       7045":
                case "DC       7050":
                case "DC       7060":
                case "DT       7020":
                case "DT       7040":
                case "DT       7045":
                case "DT       7050":
                case "DT       7060":
                case "EC       7040":
                case "EC       7050":
                case "EC       7060":
                case "EC       7440":
                case "NE       7040":
                case "NE       7050":
                case "NE       7060":
                case "RC       7050":
                    result = "000";
                    break;
                default:
                    if (fund.Length >= 3)
                    {
                        result = fund.Substring(0, 3);
                    }
                    break;
            }
            return result;
        }

        private static string GetFormattedAccountNumberForPO(string acct)
        {
            string result = "Not Found";

            switch (acct)
            {
                case "ARMGAS   7240":
                    result = "2000017240";
                    break;
                case "ARMORY   7040":
                    result = "2000017040";
                    break;
                case "ARMORY   7050":
                    result = "2000017050";
                    break;
                case "ARMORY   7060":
                    result = "2000017060";
                    break;
                case "ARMTOOLS 7230":
                    result = "2000017230";
                    break;
                case "DC       7040":
                    result = "5000017040";
                    break;
                case "DC       7045":
                    result = "5000017045";
                    break;
                case "DC       7050":
                    result = "5000017050";
                    break;
                case "DC       7060":
                    result = "5000017060";
                    break;
                case "DT       7020":
                    result = "3000017020";
                    break;
                case "DT       7040":
                    result = "3000017040";
                    break;
                case "DT       7045":
                    result = "3000017045";
                    break;
                case "DT       7050":
                    result = "3000017050";
                    break;
                case "DT       7060":
                    result = "3000017060";
                    break;
                case "EC       7040":
                    result = "4000017040";
                    break;
                case "EC       7050":
                    result = "4000017050";
                    break;
                case "EC       7060":
                    result = "4000017060";
                    break;
                case "EC       7440":
                    result = "4000017440";
                    break;
                case "NE       7040":
                    result = "6000017040";
                    break;
                case "NE       7050":
                    result = "6000017050";
                    break;
                case "NE       7060":
                    result = "6000017060";
                    break;
                case "RC       7050":
                    result = "1000017050";
                    break;

                default:
                    if (acct.Length >= 13)
                    {
                        result = acct.Substring(3, 3) + "" + acct.Substring(6, 3) + "" + acct.Substring(9, 4);
                    }
                    break;
            }
            return result;
        }

        private static string GetGAUAccountNumberForPO(string acct)
        {
            string result = "Not Found";

            switch (acct)
            {
                case "ARMGAS   7240":
                    result = "0002000017240";
                    break;
                case "ARMORY   7040":
                    result = "0002000017040";
                    break;
                case "ARMORY   7050":
                    result = "0002000017050";
                    break;
                case "ARMORY   7060":
                    result = "0002000017060";
                    break;
                case "ARMTOOLS 7230":
                    result = "0002000017230";
                    break;
                case "DC       7040":
                    result = "0005000017040";
                    break;
                case "DC       7045":
                    result = "0005000017045";
                    break;
                case "DC       7050":
                    result = "0005000017050";
                    break;
                case "DC       7060":
                    result = "0005000017060";
                    break;
                case "DT       7020":
                    result = "0003000017020";
                    break;
                case "DT       7040":
                    result = "0003000017040";
                    break;
                case "DT       7045":
                    result = "0003000017045";
                    break;
                case "DT       7050":
                    result = "0003000017050";
                    break;
                case "DT       7060":
                    result = "0003000017060";
                    break;
                case "EC       7040":
                    result = "0004000017040";
                    break;
                case "EC       7050":
                    result = "0004000017050";
                    break;
                case "EC       7060":
                    result = "0004000017060";
                    break;
                case "EC       7440":
                    result = "0004000017440";
                    break;
                case "NE       7040":
                    result = "0006000017040";
                    break;
                case "NE       7050":
                    result = "0006000017050";
                    break;
                case "NE       7060":
                    result = "0006000017060";
                    break;
                case "RC       7050":
                    result = "0001000017050";
                    break;

                default:
                    result = acct;
                    break;
            }
            return result;
        }

        public static void ProcessRent()
        {
            StringBuilder commandText = new StringBuilder();
            StringBuilder outputTextFile = new StringBuilder();
            commandText.Append("SELECT * FROM dtl");
            Console.WriteLine(commandText.ToString());
            DateTime paidDate;
            outputTextFile.Append("CreateDate, CAPSys_ClientAssistanceId__c,CAPSys_ContactID__c,Name, Type_of_Assistance__c,Purchase_Order__c, Program__c");

            outputTextFile.Append("CAPSys_RentId__c, Amount__c,Date__c,Paid_By__c,Status__c,Type__c");
            //where year(PDATORD) is 2010 and later, pVoid is null, pstage=15
            try{
            outputTextFile.Append(Environment.NewLine);
            int k = 0;
            using (SqlConnection cn = new SqlConnection(ConfigurationManager.AppSettings["connectionString"]))
            {
                //ignore where PREFPRJNUM is not null
                cn.Open();
                using (SqlCommand cmd = cn.CreateCommand())
                {
                    cmd.CommandText = commandText.ToString();
                    using (IDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            paidDate = DateTime.MinValue;
                            if (!string.IsNullOrEmpty(GetDataReaderValue("DDATCOMP", reader).ToString().Trim()))
                            {
                                DateTime.TryParse(GetDataReaderValue("DDATCOMP", reader).ToString(), out paidDate);
                            }
                            else
                            {
                                DateTime.TryParse(GetDataReaderValue("DDATOPEN", reader).ToString(), out paidDate);

                            }

                            if (paidDate.Year >= DateTime.Now.Year - 10 && "A|D|R|T".Contains(GetDataReaderValue("dacc", reader).ToString().ToUpper()) && GetDataReaderValue("dsrv", reader).ToString().ToUpper().Trim() == "HOME" && GetDataReaderValue("dsub", reader).ToString().ToUpper().Trim() == "FINANCIAL AID")
                            {
                                Console.WriteLine("Processing Line::" + k.ToString());
                                k++;
                                //iif(dtl.dsrv='HOME' and dtl.dsub='FINANCIAL AID' and dtl.dacc$'A|D|R|T',  dtl.damt, "")
                                outputTextFile.Append(GetDataReaderValue("DIDNUM", reader).ToString().Trim());
                                outputTextFile.Append(",");
                                outputTextFile.Append(GetDataReaderValue("damt", reader).ToString().Trim());
                                outputTextFile.Append(",");
                                outputTextFile.Append(paidDate.ToString("yyyy-MM-dd"));
                                outputTextFile.Append(",\"SNAP\",\"Paid\",\"");
                                switch (GetDataReaderValue("dacc", reader).ToString().ToUpper())
                                {
                                    case "A":
                                        outputTextFile.Append("Fee");

                                        break;
                                    case "R":
                                        outputTextFile.Append("Rent");
                                        break;
                                    case "D":
                                        outputTextFile.Append("Deposit");
                                        break;
                                    case "T":
                                        outputTextFile.Append("Rent Arrears");
                                        break;
                                    default:
                                        outputTextFile.Append("Other" + GetDataReaderValue("dacc", reader).ToString().ToUpper());
                                        break;

                                }
                                outputTextFile.Append("\"");

                                outputTextFile.Append(Environment.NewLine);
                            }
                        }
                    }
                    File.AppendAllText(ConfigurationManager.AppSettings["outputDirectory"] + @"rent_payment.csv", outputTextFile.ToString());
                }
            }
            }
            catch (Exception ex)
            {
                File.AppendAllText(ConfigurationManager.AppSettings["outputDirectory"] + @"rent_error", string.Format("{0}-{1}{2}", ex.Message, ex.StackTrace, Environment.NewLine));

            }
        }

        public static void ProcessService()
        {
            StringBuilder commandText = new StringBuilder();
            StringBuilder outputTextFile = new StringBuilder();
            commandText.Append("SELECT * FROM dtl WHERE (not dCTR is null and not dssn1 is null and len(rtrim(ltrim(dssn1)))>0 and len(rtrim(ltrim(dctr)))>0) and lower(dCTR) <> 'a' and datediff(day, DDATOPEN, '1/1/2013')<=0");
            Console.WriteLine(commandText.ToString());
            DateTime dateCompleted;
            DateTime dateOpen;
            decimal hours;
            try{
            outputTextFile.Append("CAPSys_ServiceId__c,CAPSys_ProgramEnrollmentID__c, " +
                "LastActivityDate,Completion_Date__c,Date_of_Service__c,Hours_Spent_With_Client__c," +
                "Minutes_Spent_With_Client__c,Note__c,Service_Type__c,Worker__c");
            //where year(PDATORD) is 2010 and later, pVoid is null, pstage=15
            outputTextFile.Append(Environment.NewLine);
            int k = 0;
            using (SqlConnection cn = new SqlConnection(ConfigurationManager.AppSettings["connectionString"]))
            {
                //ignore where PREFPRJNUM is not null
                cn.Open();
                using (SqlCommand cmd = cn.CreateCommand())
                {
                    cmd.CommandText = commandText.ToString();
                    using (IDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            dateOpen = DateTime.MinValue;
                            dateCompleted = DateTime.MinValue;
                            hours = 0;
                            if (!string.IsNullOrEmpty(GetDataReaderValue("DDATCOMP", reader).ToString().Trim()))
                            {
                                DateTime.TryParse(GetDataReaderValue("DDATCOMP", reader).ToString(), out dateCompleted);
                            }
                            if (!string.IsNullOrEmpty(GetDataReaderValue("ddatopen", reader).ToString().Trim()))
                            {
                                DateTime.TryParse(GetDataReaderValue("ddatopen", reader).ToString(), out dateOpen);
                            }
                            decimal.TryParse(GetDataReaderValue("dhrs", reader).ToString(), out hours);

                            if ((GetDataReaderValue("dsrv", reader).ToString().Trim().ToUpper() == "HOME"))
                            {
                                Console.WriteLine("Processing Line::" + k.ToString());
                                k++;

                                outputTextFile.Append("HOM" + GetDataReaderValue("DIDNUM", reader).ToString().Trim());
                                outputTextFile.Append(",");
                                outputTextFile.Append(GetDataReaderValue("DIDNUM", reader).ToString().Trim());
                                outputTextFile.Append(",");
                                if (dateCompleted != DateTime.MinValue)
                                {
                                    outputTextFile.Append(dateCompleted.ToString("yyyy-MM-dd"));
                                }
                                outputTextFile.Append(",");
                                if (dateCompleted != DateTime.MinValue)
                                {
                                    outputTextFile.Append(dateCompleted.ToString("yyyy-MM-dd"));
                                }
                                outputTextFile.Append(",");
                                if (dateOpen != DateTime.MinValue)
                                {
                                    outputTextFile.Append(dateOpen.ToString("yyyy-MM-dd"));
                                }
                                outputTextFile.Append(",");
                                outputTextFile.Append(hours.ToString());
                                outputTextFile.Append(",");
                                outputTextFile.Append((hours * 60).ToString());
                                outputTextFile.Append(",\"");
                                outputTextFile.Append(GetDataReaderValue("dmemo", reader).ToString().Replace("\"", "'").Replace("\r", " ").Replace("\n", " ").Trim());
                                outputTextFile.Append("\",\"");
                                switch (GetDataReaderValue("dsub", reader).ToString().Trim().ToUpper())
                                {
                                        case "CLSVC":
                                        case "VISIT":
                                        case "AFTERCARE":
                                            outputTextFile.Append("Counseling Session");
                                            break;
                                        case "CLSVC DIVE":
                                        outputTextFile.Append("Diversion");
                                        break;
                                    case "MEDITATION":
                                        outputTextFile.Append("LL Mediation");
                                        break;
                                    case "SHCA":
                                        outputTextFile.Append("Homeless Intake");
                                        break;
                                    case "FINANCIAL AID":
                                        outputTextFile.Append("Financial Aid");
                                        break;
                                    default:
                                        outputTextFile.Append("Counseling Session");
                                        break;
                                }
                                outputTextFile.Append("\",");
                                outputTextFile.Append(GetDataReaderValue("DWKRID", reader).ToString().Trim());

                                outputTextFile.Append(Environment.NewLine);
                            }
                        }
                    }
                    File.AppendAllText(ConfigurationManager.AppSettings["outputDirectory"] + @"homeless_service.csv", outputTextFile.ToString());
                }
            }
            }
            catch (Exception ex)
            {
                File.AppendAllText(ConfigurationManager.AppSettings["outputDirectory"] + @"service_error", string.Format("{0}-{1}{2}", ex.Message, ex.StackTrace, Environment.NewLine));

            }
        }

        public static void ProcessClientAssistance()
        {
            StringBuilder commandText = new StringBuilder();
            StringBuilder outputTextFile = new StringBuilder();
            commandText.Append("select * FROM DTL where dsrv='HOME' and DSUB='FINANCIAL AID' and (datediff(y, ddatopen, '1/1/2010') <= 0 OR datediff(y, DDATCOMP, '1/1/2010') <= 0) and (DACC IN('M', 'E', 'O', 'U','A','D','R','T') OR DEXT IN('T', 'R', 'L'))");
            Console.WriteLine(commandText.ToString());
            DateTime dateCompleted;
            DateTime dateOpen;
            decimal hours;
            outputTextFile.Append("CreateDate, CAPSys_ClientAssistanceId__c,CAPSys_ContactID__c,Name, Type_of_Assistance__c,Purchase_Order__c, Program__c");
            //where year(PDATORD) is 2010 and later, pVoid is null, pstage=15
            IDictionary<string, string> ids = new Dictionary<string, string>();
            IDictionary<string, string> names = new Dictionary<string, string>();
            try
            {
                outputTextFile.Append(Environment.NewLine);
                int k = 0;
                Console.WriteLine("Loading Member Names");
                using (SqlConnection cn = new SqlConnection(ConfigurationManager.AppSettings["connectionString"]))
                {
                    //ignore where PREFPRJNUM is not null
                    cn.Open();
                    using (SqlCommand cmd = cn.CreateCommand())
                    {
                        cmd.CommandText = "SELECT * FROM mem WHERE mseq !=4";
                        using (IDataReader reader = cmd.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                if (!ids.ContainsKey(GetDataReaderValue("mssn", reader).ToString().Trim()))
                                {
                                    ids.Add(GetDataReaderValue("mssn", reader).ToString().Trim(), GetDataReaderValue("MIDNUM", reader).ToString().Trim());
                                    names.Add(GetDataReaderValue("mssn", reader).ToString().Trim(), GetDataReaderValue("MLAST", reader).ToString().Trim() +  ", " + GetDataReaderValue("MFIRST", reader).ToString().Trim());
                                }
                            }
                        }
                    }
                }
                string id = string.Empty;
                int i = 0;
                using (SqlConnection cn = new SqlConnection(ConfigurationManager.AppSettings["connectionString"]))
                {
                    //ignore where PREFPRJNUM is not null
                    cn.Open();
                    using (SqlCommand cmd = cn.CreateCommand())
                    {
                        cmd.CommandText = commandText.ToString();
                        using (IDataReader reader = cmd.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                id = GetDataReaderValue("dpo", reader).ToString().Trim();
                                i++;
                                dateOpen = DateTime.MinValue;
                                dateCompleted = DateTime.MinValue;
                                hours = 0;
                                if (!string.IsNullOrEmpty(GetDataReaderValue("DDATCOMP", reader).ToString().Trim()))
                                {
                                    DateTime.TryParse(GetDataReaderValue("DDATCOMP", reader).ToString(), out dateCompleted);
                                }
                                if (!string.IsNullOrEmpty(GetDataReaderValue("ddatopen", reader).ToString().Trim()))
                                {
                                    DateTime.TryParse(GetDataReaderValue("ddatopen", reader).ToString(), out dateOpen);
                                }

                                
                                    Console.WriteLine("Processing Line::" + k.ToString());
                                    k++;
                                    outputTextFile.Append("\"" + dateOpen.ToString("yyyy-MM-dd")); //id
                                    outputTextFile.Append("\", \"HOM" + GetDataReaderValue("DIDNUM", reader).ToString().Trim()); //id
                                    outputTextFile.Append("\",\"MEM");
                                    if (ids.ContainsKey(GetDataReaderValue("dssn", reader).ToString().Trim()))//client
                                    {
                                        outputTextFile.Append(ids[GetDataReaderValue("dssn", reader).ToString().Trim()]);
                                    }
                                    outputTextFile.Append("\",\"");
                                    if (ids.ContainsKey(GetDataReaderValue("dssn", reader).ToString().Trim()))//client
                                    {
                                        outputTextFile.Append(names[GetDataReaderValue("dssn", reader).ToString().Trim()]);
                                    }
                                    outputTextFile.Append("\",\"");
                                    switch (GetDataReaderValue("dacc", reader).ToString().Trim().ToUpper())//type of assistance
                                    {
                                        case "M":
                                            outputTextFile.Append("Emergency Hotel");
                                            break;
                                        case "E":
                                            outputTextFile.Append("Employment Supplies");
                                            break;
                                        case "A":
                                            outputTextFile.Append("Rent Fee");
                                            break;
                                        case "R":
                                            outputTextFile.Append("Rent");
                                            break;
                                        case "D":
                                            outputTextFile.Append("Rent Deposit");
                                            break;
                                        case "T":
                                            outputTextFile.Append("Rent Arrears");
                                            break;
                                        case "O":
                                            switch (GetDataReaderValue("dext", reader).ToString().Trim().ToUpper())//type of assistance
                                            {                                               
                                                case "T":
                                                    outputTextFile.Append("Bus Pass");
                                                    break;
                                                case "R":
                                                    outputTextFile.Append("Car Repair");
                                                    break;
                                                case "L":
                                                    outputTextFile.Append("Driver's License");
                                                    break;
                                                case "O":
                                                    outputTextFile.Append("Other");
                                                    break;
                                                default:
                                                    outputTextFile.Append("Other");
                                                    break;
                                            }
                                            break;
                                        default:
                                            switch (GetDataReaderValue("dext", reader).ToString().Trim().ToUpper())//type of assistance
                                            {
                                                
                                                case "T":
                                                    outputTextFile.Append("Bus Pass");
                                                    break;
                                                case "R":
                                                    outputTextFile.Append("Car Repair");
                                                    break;
                                                case "L":
                                                    outputTextFile.Append("Driver's License");
                                                    break;
                                                case "O":
                                                    outputTextFile.Append("Other");
                                                    break;
                                                default:
                                                    outputTextFile.Append("Other");
                                                    break;
                                            }
                                            break;
                                    }
                                    outputTextFile.Append("\",\"");
                                    outputTextFile.Append(GetDataReaderValue("dpo", reader).ToString().Trim());//purchase order
                                    outputTextFile.Append("\", \"Homeless\"");

                                    outputTextFile.Append(Environment.NewLine);
                                
                            }
                        }
                        File.AppendAllText(ConfigurationManager.AppSettings["outputDirectory"] + @"homeless_client_assistance.csv", outputTextFile.ToString());
                    }
                }
            }
            catch (Exception ex)
            {
                File.AppendAllText(ConfigurationManager.AppSettings["outputDirectory"] + @"client_assistance_error", string.Format("{0}-{1}{2}", ex.Message, ex.StackTrace, Environment.NewLine));

            }
        }

        public static void ProcessUser()
        {
            StringBuilder commandText = new StringBuilder();
            StringBuilder outputTextFile = new StringBuilder();
            commandText.Append("SELECT * FROM emp");
            Console.WriteLine(commandText.ToString());
            bool salary = false;
            try{
            outputTextFile.Append("CAPSys_UserId__c,EmployeeNumber,FirstName,LastName,Grade__c,Payment_Cycle__c,Payment_Type__c,Step__c,Wage_type__c,IsActive,Worker_Number__c");
            //where year(PDATORD) is 2010 and later, pVoid is null, pstage=15
            outputTextFile.Append(Environment.NewLine);
            int k = 0;
            using (SqlConnection cn = new SqlConnection(ConfigurationManager.AppSettings["connectionString"]))
            {
                //ignore where PREFPRJNUM is not null
                cn.Open();
                using (SqlCommand cmd = cn.CreateCommand())
                {
                    cmd.CommandText = commandText.ToString();
                    using (IDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            if (GetDataReaderValue("esal", reader).ToString().ToUpper().Trim() == "S")
                            {
                                salary = true;
                            }
                            else
                            {
                                salary = false;

                            }
                            Console.WriteLine("Processing Line::" + k.ToString());
                            k++;
                            //iif(dtl.dsrv='HOME' and dtl.dsub='FINANCIAL AID' and dtl.dacc$'A|D|R|T',  dtl.damt, "")
                            outputTextFile.Append(GetDataReaderValue("enum", reader).ToString().Trim());
                            outputTextFile.Append(",\"");
                            outputTextFile.Append(GetDataReaderValue("enum", reader).ToString().Trim());
                            outputTextFile.Append("\",\"");
                            outputTextFile.Append(GetDataReaderValue("efirst", reader).ToString().Trim());
                            outputTextFile.Append("\",\"");
                            outputTextFile.Append(GetDataReaderValue("elast", reader).ToString().Trim());
                            outputTextFile.Append("\",");
                            if (GetDataReaderValue("egrd", reader).ToString().Length >= 3)
                            {
                                outputTextFile.Append(GetDataReaderValue("egrd", reader).ToString().Substring(0, 2).Trim());
                            }

                            outputTextFile.Append(",\"");
                            outputTextFile.Append(GetDataReaderValue("eprd", reader).ToString().Trim());
                            outputTextFile.Append("\",\"");
                            if (salary)
                            {
                                outputTextFile.Append("With Exempt Time");
                            }
                            else
                            {
                                outputTextFile.Append("With Overtime");
                            }
                            outputTextFile.Append("\",\"");
                            if (GetDataReaderValue("egrd", reader).ToString().Length >= 3)
                            {
                                outputTextFile.Append(GetDataReaderValue("egrd", reader).ToString().Substring(2).Trim());
                            }
                            outputTextFile.Append("\",\"");
                            if (salary)
                            {
                                outputTextFile.Append("Salary");
                            }
                            else
                            {
                                outputTextFile.Append("Hourly");
                            }
                            outputTextFile.Append("\",\"");
                            //if eloc=xx then IsActive=fase otherwise IsActive=true
                            if (GetDataReaderValue("eloc", reader).ToString().Trim().ToUpper().Contains("XX"))
                            {
                                outputTextFile.Append("No");
                            }
                            else
                            {
                                outputTextFile.Append("Yes");
                            }
                            outputTextFile.Append("\",");
                            outputTextFile.Append(GetDataReaderValue("enum", reader).ToString().Trim());
                            outputTextFile.Append(Environment.NewLine);
                        }

                    }
                    File.AppendAllText(ConfigurationManager.AppSettings["outputDirectory"] + @"user.csv", outputTextFile.ToString());
                }
            }
            }
            catch (Exception ex)
            {
                File.AppendAllText(ConfigurationManager.AppSettings["outputDirectory"] + @"user_error", string.Format("{0}-{1}{2}", ex.Message, ex.StackTrace, Environment.NewLine));

            }
        }

        public static void ProcessTimesheetGroupMember()
        {
            try
            {
                StringBuilder commandText = new StringBuilder();
                StringBuilder outputTextFile = new StringBuilder();
                commandText.Append("SELECT * FROM emp");
                Console.WriteLine(commandText.ToString());
                DateTime paidDate;
                outputTextFile.Append("CAPSys_GRP_ID__C,CAPSys_UserId__c");
                //where year(PDATORD) is 2010 and later, pVoid is null, pstage=15
                outputTextFile.Append(Environment.NewLine);
                int k = 0;
                using (SqlConnection cn = new SqlConnection(ConfigurationManager.AppSettings["connectionString"]))
                {
                    string[] groups;
                    //ignore where PREFPRJNUM is not null
                    cn.Open();
                    using (SqlCommand cmd = cn.CreateCommand())
                    {
                        cmd.CommandText = commandText.ToString();
                        using (IDataReader reader = cmd.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                Console.WriteLine("Processing Line::" + k.ToString());
                                if (!GetDataReaderValue("eloc", reader).ToString().Trim().ToUpper().Contains("XX"))
                                {
                                    if (GetDataReaderValue("EGRPS", reader).ToString().Trim().ToUpper().Contains("|"))
                                    {
                                        groups = GetDataReaderValue("EGRPS", reader).ToString().Trim().ToUpper().Split('|');

                                        foreach (string group in groups)
                                        {
                                            outputTextFile.Append("\"");
                                            outputTextFile.Append(group);
                                            outputTextFile.Append("\",");
                                            outputTextFile.Append(GetDataReaderValue("enum", reader).ToString().Trim());
                                            outputTextFile.Append(Environment.NewLine);
                                        }
                                    }
                                    else
                                    {
                                        outputTextFile.Append("\"");
                                        outputTextFile.Append(GetDataReaderValue("EGRPS", reader).ToString().Trim().ToUpper());
                                        outputTextFile.Append("\",");
                                        outputTextFile.Append(GetDataReaderValue("enum", reader).ToString().Trim());
                                        outputTextFile.Append(Environment.NewLine);
                                    }

                                }
                            }
                        }
                        File.AppendAllText(ConfigurationManager.AppSettings["outputDirectory"] + @"TC_TimesheetGroupMember.csv", outputTextFile.ToString());
                    }

                }
            }
            catch (Exception ex)
            {
                File.AppendAllText(ConfigurationManager.AppSettings["outputDirectory"] + @"TC_TimesheetGroupMember_error", string.Format("{0}-{1}{2}", ex.Message, ex.StackTrace, Environment.NewLine));

            }
        }

        public static void ProcessCrystalReport(string reportName)
        {
            try
            {
                ReportDocument report = new ReportDocument();
                Console.WriteLine("Processing :: {0}", reportName);
                report.Load(reportName);
                ExportOptions options = new ExportOptions();
                options.ExportFormatType = ExportFormatType.CharacterSeparatedValues;
                ExportDestinationOptions destOptions = new DiskFileDestinationOptions()
                {
                    DiskFileName = reportName.Replace(ConfigurationManager.AppSettings["crystalReportsPath"], ConfigurationManager.AppSettings["outputDirectory"]).Replace("rpt", "csv")
                };

                //CharacterSeparatedValuesFormatOptions v= ExportOptions.CreateCharacterSeparatedValuesFormatOptions();
                //v.SeparatorText

                options.ExportFormatOptions = new CharacterSeparatedValuesFormatOptions()
                {
                    ExportMode = CsvExportMode.Standard,
                    GroupSectionsOption = CsvExportSectionsOption.ExportIsolated,
                    ReportSectionsOption = CsvExportSectionsOption.ExportIsolated,
                  
                };

                options.ExportDestinationOptions = destOptions;
                
                for (int i = 0; i < report.DataSourceConnections.Count; i++)
                {

                    report.DataSourceConnections[i].SetConnection(ConfigurationManager.AppSettings["crystalServer"], ConfigurationManager.AppSettings["crystalDB"], "crystal", "crystal");

                }
                if (!Directory.Exists(Path.GetDirectoryName(reportName.Replace(ConfigurationManager.AppSettings["crystalReportsPath"], ConfigurationManager.AppSettings["outputDirectory"]))))
                {
                    Directory.CreateDirectory(Path.GetDirectoryName(reportName.Replace(ConfigurationManager.AppSettings["crystalReportsPath"], ConfigurationManager.AppSettings["outputDirectory"])));
                }
                
                report.Export(options);
            }
            catch (Exception ex)
            {
                File.AppendAllText(ConfigurationManager.AppSettings["outputDirectory"] + "ErrorLog.txt", string.Format("Error on Report::{0} Message::{1} Stack{2}{3}", reportName, ex.Message, ex.StackTrace, Environment.NewLine));

            }
        }
    }
    
}
