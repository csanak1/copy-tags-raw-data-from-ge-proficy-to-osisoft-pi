using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CopyTagsRawDataFromGEProficyToOsisoftPI
{
    class Program
    {
        private static readonly HistDA histDA = new HistDA();
        private static readonly PIDA piDA = new PIDA();

        static void Main(string[] args)
        {
            Console.WriteLine("     +++ BorsodChem Zrt. +++      ");
            Console.WriteLine("Copy Tag Raw values from GE Proficy MES to OSIsoft PI");
            Console.WriteLine("At this point this project is for test purposes only!");
            Console.WriteLine("");

            StartDataProcessFromProficyHistorian();
            //DeleteValuesFromPISystem();

            Console.ReadKey();
        }

        private static void StartDataProcessFromProficyHistorian()
        {
            //string piTag = "BC.M.MANUAL_TEST.PV";     //Float32 test Tag in PI:   BC.M.MANUAL_TEST.PV
            //string piTag = "BC.M.MANUAL_TEST2.PV";    //String test Tag in PI:    BC.M.MANUAL_TEST2.PV
            //string piTag = "BC.M.MANUAL_TEST3.PV";    //Digital test Tag in PI:   BC.M.MANUAL_TEST3.PV -> for motors/pumps/valves open-close positions
            //string piTag = "BC.M.MANUAL_TEST4.PV";    //Int16 test Tag in PI:   BC.M.MANUAL_TEST4.PV

            //Data Type:                                       GE Proficy       ->      Osisoft PI
            //string tagName = "BC.TDI1.FIC3401.PV";        // Single Float     ->      Float32 
            //string tagName = "BC.TDI1.FIC3401.MODE";      // Variable String  ->      String
            //string tagName = "BC.HCLCONV.FIC1020.MODE";   // (python script)  ->      String
            //string tagName = "BC.TDI1.P3774BFUT.PV";      // Single Float     ->      Digital (running signal:    0;1)
            //string tagName = "BC.TDI2.UP6901A.PV";        // Single Integer   ->      Digital (running signal:    0;2 -> 0 is not running always)
            //string tagName = "BC.HCLCONV.XV1700B.PV";     // Single Float     ->      Digital (running signal:    -1;0 -> 0 is not running always)

            string[] tagNames = { "BC.M.PITEST1.PV", "BC.M.PITEST2.PV", "BC.M.PITEST3.PV" };
            //string[] tagNames = { "BC.TDI1.FIC3401.PV" };

            var startDate = new DateTime(2022, 01, 01, 00, 00, 00); // give a correct start date
            //var startDate = DateTime.Now.AddHours(-12);
            //var startDate = DateTime.Now.AddDays(-120);

            //var endDate = new DateTime(2022, 01, 01, 00, 00, 00); // give a correct end date
            var endDate = DateTime.Now;

            foreach (string tagName in tagNames)
            {
                Console.WriteLine("");
                Console.WriteLine("Tag: " + tagName);
                Console.WriteLine("Query start date: " + startDate.ToString("yyyy.MM.dd. HH:mm:ss"));

                var rawData = ProcessDataFromProficy(tagName, startDate, endDate);

                var dataToImport = rawData.AsEnumerable()
                    .Where(r => r.Field<DateTime>("TimeStamp") <= endDate)
                    //.Where(r => r.Field<string>("Quality") == "Good")
                    .Select
                    (r => new
                    {
                        Value = r.Field<string>("Value"),
                        Quality = r.Field<string>("Quality"),
                        TimeStamp = r.Field<DateTime>("TimeStamp").ToLocalTime().IsDaylightSavingTime() ?
                            r.Field<DateTime>("TimeStamp").ToLocalTime() :
                            r.Field<DateTime>("TimeStamp").ToLocalTime().AddHours(-1) //UTC -> CET - CEST correction!!!
                    }).ToList();

                if (dataToImport.Count > 0)
                {
                    Console.WriteLine("Start inserting values into AF database!");

                    //bool result = piDA.UpdateTagValues(piTag, Extensions.ConvertToDataTable(dataToImport), true); //for TEST Tags, MUST BE added manually
                    //bool result = piDA.UpdateTagValues(tagName, Extensions.ConvertToDataTable(dataToImport), false);
                    //bool result = piDA.InsertTagValues(piTag, Extensions.ConvertToDataTable(dataToImport), false);
                    bool result = piDA.InsertTagValues(tagName, Extensions.ConvertToDataTable(dataToImport), false);

                    if (result)
                    {
                        Console.WriteLine("Insert is finished!");
                    }
                }
                else
                {
                    Console.WriteLine("No data to be written! Check your query!");
                }
            }

            Console.WriteLine("Finished all tags.");
        }

        private static DataTable ProcessDataFromProficy(string tagName, DateTime startDate, DateTime endDate)
        {
            int rowCount = 0;
            var tagRawData = new DataTable();

            Console.WriteLine();
            Console.WriteLine("Start");
            Console.WriteLine();

            if (!histDA.IsConnected)
            {
                histDA.Connect();

                do
                {
                    var tagRawDataPart = histDA.QueryRawTagValues(startDate, tagName, out string dataType);

                    startDate = tagRawDataPart.AsEnumerable()
                        .Max(r => r.Field<DateTime>("TimeStamp"));

                    tagRawData.Merge(tagRawDataPart);

                    rowCount = tagRawData.Rows.Count;

                    Console.Write("\rQuery progress: {0} row(s)", rowCount);

                    //Thread.Sleep(5000);
                }
                while (rowCount % 500000 == 0 && rowCount > 0 && rowCount < 16500000 && startDate < endDate);

                var badValuesCount = tagRawData.AsEnumerable()
                        .Count(r => r.Field<string>("Quality") != "Good");

                var minDate = tagRawData.AsEnumerable()
                        .Min(r => r.Field<DateTime>("TimeStamp"));

                var maxDate = tagRawData.AsEnumerable()
                        .Max(r => r.Field<DateTime>("TimeStamp"));

                Console.WriteLine();
                Console.WriteLine();
                Console.WriteLine("Done!");
                Console.WriteLine("Out of this BAD quality values: " + badValuesCount);
                Console.WriteLine("Min date (UTC): " + minDate.ToString("yyyy.MM.dd. HH:mm:ss"));
                Console.WriteLine("Max date (UTC): " + maxDate.ToString("yyyy.MM.dd. HH:mm:ss"));
                Console.WriteLine("!!! TimeStamp is in UTC time, just like how GE Proficy stores values in its own database! Date is converted to local during the next step!");
            }

            return tagRawData;
        }

        private static void DeleteValuesFromPISystem()
        {
            string piTag = "BC.M.MANUAL_TEST3.PV";     //Float32 test Tag in PI:   BC.M.MANUAL_TEST.PV

            Console.WriteLine("Delete method started for tag: " + piTag);

            var startDate = new DateTime(2021, 01, 18, 12, 52, 50, DateTimeKind.Local); // give a correct start date
            var endDate = new DateTime(2022, 01, 19, 13, 52, 50, DateTimeKind.Local); // give a correct end date

            Console.WriteLine("Delete start: " + startDate.ToString("yyyy.MM.dd. HH:mm:ss"));
            Console.WriteLine("Delete end: " + endDate.ToString("yyyy.MM.dd. HH:mm:ss"));

            var values = piDA.QueryTagRawValues(piTag, startDate, endDate);
            //var values = piDA.QueryTagRawValues(piTag, startDate, 1, true);

            if (values.Count != 0)
            {
                var result = piDA.DeleteTagValues(values);

                if (result)
                {
                    Console.WriteLine(values.Count + " value(s) are deleted!");
                }
                else
                {
                    Console.WriteLine("No values are deleted!");
                }
            }
            else
            {
                Console.WriteLine("No data can be deleted for the selected interval!");
            }
        }
    }
}
