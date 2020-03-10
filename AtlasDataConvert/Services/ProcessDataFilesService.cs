using AtlasDataConvert.Models;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace AtlasDataConvert.Services
{
    public static class ProcessDataFilesService
    {
        public static DatabaseTableModel LoadTableModel(string databaseTablePath)
        {
            DatabaseTableModel result = null;
            XmlSerializer serializer = new XmlSerializer(typeof(DatabaseTableModel));
            serializer.UnknownNode += new
            XmlNodeEventHandler(serializer_UnknownNode);
            serializer.UnknownAttribute += new
            XmlAttributeEventHandler(serializer_UnknownAttribute);

            FileStream fs = new FileStream(databaseTablePath, FileMode.Open);

            result = (DatabaseTableModel)serializer.Deserialize(fs);
            return result;
        }

        private static void serializer_UnknownNode(object sender, XmlNodeEventArgs e)
        {
            LogService.LogMessage("Unknown Node:" + e.Name + "\t" + e.Text);
        }

        private static void serializer_UnknownAttribute(object sender, XmlAttributeEventArgs e)
        {
            System.Xml.XmlAttribute attr = e.Attr;
            LogService.LogMessage("Unknown attribute " + attr.Name + "='" + attr.Value + "'");
        }
        public static bool GenerateModelXml(DatabaseTableModel tableModel, string outputFileName)
        {
            bool success = false;
            try
            {
                XmlSerializer serializer = new XmlSerializer(typeof(DatabaseTableModel));
                TextWriter writer = new StreamWriter(outputFileName);
                serializer.Serialize(writer, tableModel);
                writer.Close();
                success = true;
            }
            catch (Exception ex)
            {
                LogService.LogMessage(string.Format("Error::{0}{1}{2}{3}{2}", System.DateTime.Now, ex.Message, Environment.NewLine, ex.StackTrace));
            }
            return success;
        }


    }
}
