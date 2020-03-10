using System;
using System.Collections.Generic;
using System.IO;
using AtlasDataConvert.Models;
using AtlasDataConvert.Services;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace AtlasDataConvertTests
{
    [TestClass]
    public class DatabaseTableTests
    {
        [TestMethod]
        public void GenerateXml()
        {
            DatabaseTableModel table = new DatabaseTableModel();
            File.Delete(AppDomain.CurrentDomain.BaseDirectory + @"\MyFile.xml");
            table.SourceConnectionString = AppDomain.CurrentDomain.BaseDirectory + @"\myfile.csv";
            table.SourceDatabaseType = AtlasDataConvert.Enumerations.DatabaseType.CSV;
            table.SourceName = "MyFile";
            table.DestinationConnectionString = AppDomain.CurrentDomain.BaseDirectory + @"\myfleConverted.csv";
            table.DestinationDatabaseType = AtlasDataConvert.Enumerations.DatabaseType.CSV;
            table.DestinationName = "MyFileConverted";

            table.Fields = new List<DatabaseField>();
            table.Fields.Add(new DatabaseField() { SourceName = "Field1", DestinationName = "FieldConverted1" });
            table.Fields.Add(new DatabaseField() { SourceName = "Field2", DestinationName = "FieldConverted2" });
            table.Fields.Add(new DatabaseField() { SourceName = "Field3", DestinationName = "FieldConverted3" });
            
            ProcessDataFilesService.GenerateModelXml(table, AppDomain.CurrentDomain.BaseDirectory + @"\MyFile.xml");
        }
        [TestMethod]
        public void LoadXml()
        {
            DatabaseTableModel table = new DatabaseTableModel();

            table = ProcessDataFilesService.LoadTableModel(@"C:\Development\ProjectAtlas\DataConversion\AtlasDataConvert\MappingFIles\MyFile.xml");
        }

    }
}
