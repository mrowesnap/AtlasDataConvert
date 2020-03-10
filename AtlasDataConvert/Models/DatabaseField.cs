using AtlasDataConvert.Enumerations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AtlasDataConvert.Models
{
    public class DatabaseField
    {
        public string SourceName { get; set; }
        public string DestinationName { get; set; }
        public DataType FieldDataType { get; set; }
        public string DataLength { get; set; }
        
    }
}
