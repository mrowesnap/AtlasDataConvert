using AtlasDataConvert.Enumerations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace AtlasDataConvert.Models
{
    [XmlRootAttribute("DatabaseTableModel", Namespace = "http://www.cpandl.com", IsNullable = false)]
    public class DatabaseTableModel
    {

        public string SourceConnectionString { get; set; }
        public DatabaseType SourceDatabaseType { get; set; }
        public string SourceName { get; set; }
        public CommandType SourceCommandType { get; set; }
        public string DestinationConnectionString { get; set; }
        public DatabaseType DestinationDatabaseType { get; set; }
        public string DestinationName { get; set; }

        [XmlArrayAttribute("Fields")]
        public List<DatabaseField> Fields { get; set; }
    }       
}
