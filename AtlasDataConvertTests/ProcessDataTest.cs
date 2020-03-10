using System;
using AtlasDataConvert.Services;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace AtlasDataConvertTests
{
    [TestClass]
    public class ProcessDataTest
    {
        [TestMethod]
        public void ProcessSQL()
        {
            ProcessDataConversion.ProcessXml(@"C:\Development\ProjectAtlas\DataConversion\resultsPlus\Templates\Xml\Contacts.xml");
        }
        [TestMethod]
        public void ProcessCSV()
        {
            ProcessDataConversion.ProcessXml(@"C:\Development\ProjectAtlas\DataConversion\resultsPlus\Templates\Xml\ContactsCsv.xml");
        }
    }
}
