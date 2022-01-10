using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Historian = Proficy.Historian.ClientAccess.API; // v7.1

namespace CopyTagsRawDataFromGEProficyToOsisoftPI
{
    class HistDA
    {
        internal class TagData
        {
            public string Value { get; set; }
            public string Quality { get; set; }
            public DateTime TimeStamp { get; set; }
        }

        private const string iHistSrv = "WHBC-DB-IH";

        public string IHistSrv
        {
            get
            {
                return iHistSrv;
            }

            //TODO: setter
        }

        //variable to hold the Historian server connection
        Historian.ServerConnection sc;

        public bool IsConnected
        {
            get { return sc != null && sc.IsConnected(); }
        }

        public DataTable QueryRawTagValues(DateTime queryStart, string tagName, out string dataType)
        {
            dataType = string.Empty;

            Connect();

            if (!IsConnected)
                Connect();

            Historian.TagQueryParams queryTags = new Historian.TagQueryParams { PageSize = 500 };
            Historian.ItemErrors itemErrors = new Historian.ItemErrors();
            Historian.DataSet dataSet = new Historian.DataSet();

            List<Historian.Tag> histTagData = new List<Historian.Tag>();
            List<Historian.Tag> tempTagData;

            queryTags.Categories = Historian.Tag.Categories.Engineering;

            //Historian.DataQueryParams queryValues =
            //    new Historian.RawByTimeQuery(queryStart, tagName)
            //    { Fields = Historian.DataFields.Time | Historian.DataFields.Value | Historian.DataFields.Quality, PageSize = 500 };

            Historian.DataQueryParams queryValues =
                new Historian.RawByNumberQuery(queryStart, 500000, tagName)
                { Fields = Historian.DataFields.Time | Historian.DataFields.Value | Historian.DataFields.Quality, PageSize = 500 };

            try
            {
                while (sc.ITags.Query(ref queryTags, out tempTagData))
                    histTagData.AddRange(tempTagData);
                histTagData.AddRange(tempTagData);

                switch (histTagData[0].DataType)
                {
                    case Historian.Tag.NativeDataType.VariableString:
                        dataType = "string";
                        break;
                    case Historian.Tag.NativeDataType.FixedString:
                        dataType = "string";
                        break;
                    case Historian.Tag.NativeDataType.Bool:
                        dataType = "bool";
                        break;
                    case Historian.Tag.NativeDataType.Integer:
                        dataType = "int";
                        break;
                    case Historian.Tag.NativeDataType.DoubleInteger:
                        dataType = "int";
                        break;
                    case Historian.Tag.NativeDataType.QuadInteger:
                        dataType = "int";
                        break;
                    case Historian.Tag.NativeDataType.DoubleFloat:
                        dataType = "float";
                        break;
                    case Historian.Tag.NativeDataType.Float:
                        dataType = "float";
                        break;
                    default:
                        dataType = "na";
                        break;
                }

                dataSet.Clear();

                Historian.DataSet finalSet = new Historian.DataSet();

                while (sc.IData.Query(ref queryValues, out dataSet, out _))
                    finalSet.AddRange(dataSet);
                finalSet.AddRange(dataSet);

                var tagData = ProcessHistValues(finalSet, tagName, dataType);

                return Extensions.ConvertToDataTable(tagData);
            }
            catch(Exception e)
            {
                Console.WriteLine("Error occurred during query: " + e.Message);

                return null;
            }
            finally
            {
                Disconnect();
            } 
        }

        public List<TagData> ProcessHistValues(Historian.DataSet finalSet, string tagName, string dataType)
        {
            var tagData = new List<TagData>();

            switch (dataType) 
            {
                case "float":
                    for (int i = 0; i < finalSet[tagName].Count(); i++)
                    {
                        var data = new TagData
                        {
                            Value = finalSet[tagName].GetValue(i) == null ? string.Empty : Convert.ToDecimal(finalSet[tagName].GetValue(i)).ToString("N", CultureInfo.GetCultureInfo("hu-HU")), //the decimal separator is dot in the US or UK, etc. (decimal point), comma in Hungary, Germany, etc.
                            Quality = finalSet[tagName].GetQuality(i).ToString().ToUpper(),
                            TimeStamp = finalSet[tagName].GetTime(i)
                        };

                        tagData.Add(data);
                    }
                    break;
                default:
                    for (int i = 0; i < finalSet[tagName].Count(); i++)
                    {
                        var data = new TagData
                        {
                            Value = finalSet[tagName].GetValue(i) == null ? string.Empty : Convert.ToString(finalSet[tagName].GetValue(i)),
                            Quality = finalSet[tagName].GetQuality(i).ToString(),
                            TimeStamp = finalSet[tagName].GetTime(i)
                        };

                        tagData.Add(data);
                    }
                    break;
            }

            return tagData;
        }

        public void Connect()
        {
            if (sc == null)
            {
                sc = new Historian.ServerConnection(new Historian.ConnectionProperties
                {
                    ServerHostName = IHistSrv,
                    OpenTimeout = new TimeSpan(0, 0, 10),
                    ReceiveTimeout = new TimeSpan(0, 5, 0),
                    ServerCertificateValidationMode = Historian.CertificateValidationMode.None
                });
            }

            if (!IsConnected)
            {
                try
                {
                    sc.Connect();
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error at connecting to Historian server: " + ex.Message);
                }
            }
        }

        public void Disconnect()
        {
            if (IsConnected)
            {
                try
                {
                    sc.Disconnect();
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error at disconnecting from Historian server: " + ex.Message);
                }
            }

            Dispose();
        }

        private void Dispose()
        {
            if (sc != null)
            {
                ((IDisposable)sc).Dispose();
            }
        }
    }
}
