using OSIsoft.AF;
using OSIsoft.AF.Asset;
using OSIsoft.AF.Data;
using OSIsoft.AF.PI;
using OSIsoft.AF.Time;

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace CopyTagsRawDataFromGEProficyToOsisoftPI
{
    class PIDA : IDisposable
    {
        #region Statics
        //private const string _piSystemName = "WHBC-PIAF";
        private const string _piServerName = "WHBC-PIDA";
        //public const string _piServerIP = "xxx.xxx.xxx.xxx";
        private const string _piDatabaseString = "BC-PIAF";

        private static readonly NetworkCredential piCred = new NetworkCredential(@"user", "password", "domain");

        private const string _dateFormat = "yyyy.MM.dd. HH:mm:ss";
        private const int _pointPageSize = 1000;

        private PIServer piServer;
        private PISystem piSystem;
        private AFDatabase piDatabase;
        #endregion

        private bool IsConnected()
        {
            if (piSystem != null && piDatabase != null)
                return piSystem.ConnectionInfo.IsConnected && piServer.ConnectionInfo.IsConnected; //returns connection status, the piServer and piSystems should be checked separately
            else return false;
        }

        public PIPoint FindTagPIPointIfExists(string tagName)
        {
            //PIPoint.FindPIPoint Method doc: https://docs.osisoft.com/bundle/af-sdk/page/html/M_OSIsoft_AF_PI_PIPoint_FindPIPoint_1.htm
            return PIPoint.FindPIPoint(piServer, tagName);
        }

        public bool InsertTagValues(string tagName, DataTable values, bool compression)
        {
            Connect();

            try
            {
                var piTag = FindTagPIPointIfExists(tagName);

                if (piTag != null)
                {
                    string piPointPath = $@"\\PIServer[{_piServerName}]\PIPoint[{piTag.Name}]";

                    //AFObject.FindObject Method doc: https://docs.osisoft.com/bundle/af-sdk/page/html/M_OSIsoft_AF_AFObject_FindObject_2.htm
                    var afObject = AFObject.FindObject(piPointPath, piDatabase);

                    //AFUpdateOption enum doc: https://docs.osisoft.com/bundle/af-sdk/page/html/T_OSIsoft_AF_Data_AFUpdateOption.htm
                    var afInsertMode = AFUpdateOption.Insert; //Add the value to the archive. Any existing values at the same time are not overwritten.

                    if (!compression)
                    {
                        afInsertMode = AFUpdateOption.InsertNoCompression; //Add the value to the archive without compression. If this value is written to the snapshot, the previous snapshot value will be written to the archive, without regard to compression settings. Note that if a subsequent snapshot value is written without the InsertNoCompression option, the value added with the InsertNoCompression option is still subject to compression.
                    }

                    //AFBufferOption enum doc: https://docs.osisoft.com/bundle/af-sdk/page/html/T_OSIsoft_AF_Data_AFBufferOption.htm
                    var afBuffer = AFBufferOption.BufferIfPossible;

                    //AFValue Constructor (AFValue) doc: https://docs.osisoft.com/bundle/af-sdk/page/html/M_OSIsoft_AF_Asset_AFValue__ctor_4.htm
                    var valuesToWrite = new List<AFValue>();

                    foreach (DataRow value in values.Rows)
                    {
                        var afTimeStamp = new AFTime("*");

                        if ((DateTime)value["TimeStamp"] != DateTime.MinValue || (DateTime)value["TimeStamp"] <= DateTime.Now) //default -> set AFTime to DateTime.Now
                        {
                            afTimeStamp = new AFTime((DateTime)value["TimeStamp"]);
                        }

                        if (afObject is AFAttribute afAttribute)
                        {
                            var afValue = new AFValue(afAttribute, value["Value"], afTimeStamp);

                            valuesToWrite.Add(afValue);
                        }
                        else
                        {
                            valuesToWrite.Clear();

                            break;
                        }
                    }

                    if (valuesToWrite.Count > 0)
                    {
                        //Perform a bulk write. Use a single local call to PI Buffer Subsystem if possible.
                        //AFListData is the class that provides the bulk write method.
                        //AFListData.UpdateValues method doc: https://docs.osisoft.com/bundle/af-sdk/page/html/M_OSIsoft_AF_Data_AFListData_UpdateValues_1.htm
                        var errors = AFListData.UpdateValues(valuesToWrite, afInsertMode, afBuffer);
                        //AFListData.UpdateValues method returns AFErrors<AFValue>: https://docs.osisoft.com/bundle/af-sdk/page/html/T_OSIsoft_AF_AFErrors_1.htm

                        if (errors != null)
                        {
                            //TODO: handle errors
                            Console.WriteLine("An error occured during inserting AFValues.");

                            return false;
                        }

                        return true;
                    }
                    else return false;

                }
                else return false;
            }
            catch(Exception e)
            {
                Console.WriteLine("Error at inserting values into PI systems: " + e.Message);

                return false;
            }
            finally
            {
                Disconnect();
            }
        }

        private void Connect()
        {
            if (!IsConnected())
            {
                try
                {
                    //piServer = new PIServers().DefaultPIServer; //connect to the default PI server
                    piServer = new PIServers()[_piServerName]; //connect to the specified server

                    if (piServer == null)
                        throw new InvalidOperationException("PI Server was not found.");

                    //piServer.Connect(piCred);
                    piServer.Connect();
                    piSystem = piServer.PISystem; //trying to get the PI system

                    if (piSystem == null)
                        piSystem = new PISystems().DefaultPISystem; //connect to the default PI system

                    //piSystem = new PISystems()[_piSystemName];

                    if (piSystem == null)
                        throw new InvalidOperationException("PI Systems was not found.");

                    piSystem.Connect(piCred);

                    piDatabase = piSystem.Databases[_piDatabaseString]; //specify PI system database

                    if (piDatabase == null)
                        throw new InvalidOperationException("PI Database was not found.");
                }
                catch (PIConnectionException ex)
                {
                    Console.WriteLine("PI connection Exception occured: " + ex.Message);
                    throw;
                }

                if (!IsConnected())
                {
                    throw new Exception("Unable connect to PI server.");
                }
            }
        }

        private void Disconnect()
        {
            if (IsConnected())
            {
                piSystem.Disconnect();
                piServer.Disconnect();
            }
        }

        private float ConvertTimeStampToFloat(DateTime timeStamp) =>
            (float)timeStamp.ToOADate() * 1000000;

        public void Dispose() => piSystem.Disconnect(); //simply disconnectingv
    }
}
