using System;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FoxProFieldCountList
{
    class Program
    {
        static void Main(string[] args)
        {

            OleDbConnection fox;
            DataTable schemaTable = null;

            StringBuilder sbCount = new StringBuilder();
            int rowCount = 0;
            if (!String.IsNullOrWhiteSpace(args[0]))
            {
                if (Directory.Exists(args[0]))
                {
                    if (args[2] == null)
                    {
                        File.AppendAllText("DataCount.csv", "Table, FieldCount, RowCount, FileSize");
                        File.AppendAllText("DataCount.csv", Environment.NewLine);
                        foreach (string fileName in Directory.GetFiles(args[0], "*.dbf"))
                        {
                            if (File.Exists("DataCount.csv"))
                            {
                                File.Delete("DataCount.csv");
                            }

                            try
                            {
                                rowCount = 0;
                                fox = new OleDbConnection();

                                Console.WriteLine("Processing {0}", Path.GetFileNameWithoutExtension(fileName));
                                fox.ConnectionString = string.Format(@"Provider=vfpoledb;Data Source={0};Collating Sequence=machine;", fileName);
                                fox.Open();
                                OleDbCommand select = new OleDbCommand();
                                select.Connection = fox;
                                select.CommandText = string.Format("SELECT * FROM {0}", Path.GetFileNameWithoutExtension(fileName));
                                FileInfo fileInfo = new FileInfo(fileName);
                                OleDbDataReader reader = select.ExecuteReader();
                                schemaTable = reader.GetSchemaTable();
                                File.AppendAllText(Path.GetFileNameWithoutExtension(fileName) + "Schema.txt", "ColumnName,DataType,AllowNull" + Environment.NewLine);

                                foreach (DataRow row in schemaTable.Rows)
                                {
                                    File.AppendAllText(Path.GetFileNameWithoutExtension(fileName) + "Schema.txt", string.Format("{0}, {1}, {2}{3}", row[0].ToString(), row[5].ToString(), row[8].ToString(), Environment.NewLine));
                                }

                                while (reader.Read())
                                {
                                    rowCount++;
                                }
                                Console.WriteLine("{0}, {1}, {2}, {3}", Path.GetFileNameWithoutExtension(fileName), reader.FieldCount.ToString(), rowCount.ToString(), fileInfo.Length.ToString());
                                File.AppendAllText("DataCount.csv", string.Format("{0}, {1}, {2}{3}", Path.GetFileNameWithoutExtension(fileName), reader.FieldCount.ToString(), rowCount.ToString(), Environment.NewLine));
                                fox.Close();
                                fox.Dispose();
                            }
                            catch (Exception exc)
                            {
                                Console.WriteLine("{0}, Error, {1}", Path.GetFileNameWithoutExtension(fileName), exc.Message);
                                File.AppendAllText("DataCount.csv", string.Format("{0}, Error, {1}{2}", Path.GetFileNameWithoutExtension(fileName), exc.Message, Environment.NewLine));

                            }
                        }
                    }
                    else
                    {
                        string fileName = string.Format("{0}{1}", args[0], args[1]);

                        try
                        {
                            rowCount = 0;
                            fox = new OleDbConnection();
                            string dateColumn = args[2];
                            string date = args[3];

                            Console.WriteLine("Processing {0}", Path.GetFileNameWithoutExtension(fileName));
                            fox.ConnectionString = string.Format(@"Provider=vfpoledb;Data Source={0};Collating Sequence=machine;", fileName);
                            fox.Open();
                            OleDbCommand select = new OleDbCommand();
                            select.Connection = fox;
                            select.CommandText = string.Format("SELECT * FROM {0} order by {1}", Path.GetFileNameWithoutExtension(fileName), dateColumn);
                            Console.WriteLine(select.CommandText);
                            FileInfo fileInfo = new FileInfo(fileName);
                            OleDbDataReader reader = select.ExecuteReader();
                            schemaTable = reader.GetSchemaTable();
                          
                            foreach (DataRow row in schemaTable.Rows)
                            {
                                
                                File.AppendAllText(Path.GetFileNameWithoutExtension(fileName) + "Schema.txt", string.Format("{0}, {1}, {2}{3}", row[0].ToString(), row[5].ToString(), row[8].ToString(), Environment.NewLine));
                            }

                            while (reader.Read())
                            {
                                if (reader.GetDateTime(reader.GetOrdinal(dateColumn)) > Convert.ToDateTime(date))
                                {
                                    rowCount++;
                                }
                            }
                            Console.WriteLine("{0}, {1}, {2}, {3}", Path.GetFileNameWithoutExtension(fileName), reader.FieldCount.ToString(), rowCount.ToString(), fileInfo.Length.ToString());
                            File.AppendAllText("DataCount.csv", string.Format("{0}, {1}, {2}{3}", Path.GetFileNameWithoutExtension(fileName), reader.FieldCount.ToString(), rowCount.ToString(), Environment.NewLine));
                            fox.Close();
                            fox.Dispose();
                        }
                        catch (Exception exc)
                        {
                            Console.WriteLine("{0}, Error, {1}", Path.GetFileNameWithoutExtension(fileName), exc.Message);
                            File.AppendAllText("DataCount.csv", string.Format("{0}, Error, {1}{2}", Path.GetFileNameWithoutExtension(fileName), exc.Message, Environment.NewLine));

                        }
                    }
                }
                
                Console.WriteLine("DOne!");
            }

        }
    }
}
