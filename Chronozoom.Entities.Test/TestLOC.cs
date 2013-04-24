using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chronozoom.Entities;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Net;
using System.IO;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Chronozoom.Entities.Test;

namespace Chronozoom.Entities.Test
{
    [TestClass]
    public class LOC : StorageTest
    {
        public static string basebatchurl = "C:\\Users\\Yitao Li\\CZJSON";
        public static Collection collectionForLOC;
        public static string batchurl = basebatchurl;
        public static SuperCollection my_sc;
        /* note: replaced with actual counts in storage */
        public static int my_timeline_count; //=1000
        public static int my_contentitem_count; // = 10000;
        public static int my_exhibit_count;  // = 10000;

        /**
         * initialize test storage
         */
        [TestInitialize]
        public void Initialize()
        {
            base.Initialize();
            my_timeline_count = _storage.Timelines.Count();
            my_contentitem_count = _storage.ContentItems.Count();
            my_exhibit_count = _storage.Exhibits.Count();
        }

        /**
         * read data from url
         */
        [TestMethod]
        public void TestLOC()
        {
            int batchnum = 12;  /* specifies which files to process (todo: avoid hard-coding this value) */
            var sc = _storage.SuperCollections.Where(c => c.Title == "LOC Data");
            foreach (SuperCollection s in sc)
                my_sc = s;                          // get previous existing supercollection, if exists
            batchurl = basebatchurl + "\\Batch" + batchnum + ".json";
            try
            {
                if (my_sc == null)
                    ToRunInFirstBatch();   // no previous superconnection found, creating new supercollection
                WriteToDb();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Console.WriteLine(e.InnerException);
                Console.WriteLine(e.StackTrace);
            }
        }


        private void ToRunInFirstBatch()
        {
            //add locdata super collection
            SuperCollection loc_sc = new SuperCollection();
            loc_sc.Id = Guid.NewGuid();
            loc_sc.Title = "LOC Data";
            loc_sc.Collections = new System.Collections.ObjectModel.Collection<Collection>();
            _storage.SuperCollections.Add(loc_sc);
            _storage.SaveChanges();
            my_sc = loc_sc;
        }


        /**
         * Writes the batch to db
         */
        public void WriteToDb()
        {
            StreamReader reader = new StreamReader(batchurl);
            string s = reader.ReadToEnd();
            reader.Close();
            JObject jobj = JObject.Parse(s);
            JArray parseArray = (JArray)jobj["batches"];
            ParseJBatch(parseArray);
        }

        /**
         * Parses a batch
         */
        private void ParseJBatch(JArray parseArray)
        {
            if (parseArray != null && parseArray.Count > 0)
            {
                foreach (JObject obj in parseArray)
                {
                    {
                        try
                        {
                            string issuesurl = (string)obj["local_url"];
                            StreamReader reader = new StreamReader(basebatchurl + issuesurl);
                            string s = reader.ReadToEnd();
                            reader.Close();
                            JObject jobj = JObject.Parse(s);
                            JArray issuesArray = (JArray)jobj["issues"];
                            ParseJIssues(issuesArray);
                            _storage.SaveChanges();
                            Console.WriteLine("Parsed" + issuesurl);
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e.Message);
                            Console.WriteLine(e.InnerException);
                            Console.WriteLine(e.StackTrace);
                            continue;
                        }
                    }
                }
            }
        }


        private void ParseJIssues(JArray issuesArray)
        {
            if (issuesArray != null && issuesArray.Count > 0)
            {

                foreach (JObject obj in issuesArray)
                {
                    try
                    {
                        Collection new_coll = null;
                        JObject temp = (JObject)obj["title"];
                        String coll_title = (string)temp["name"];
                        String title = (string)temp["name"] + "," + (String)obj["date_issued"];
                        var coll = _storage.Collections.Where(c => c.Title == coll_title);
                        foreach (Collection c in coll)
                        {
                            new_coll = c;    //add as new collection if the collection does not already exist
                        }
                        //adding this newspapers collection if it does not already exist
                        if (new_coll == null)
                        {
                            new_coll = new Collection();
                            new_coll.Id = Guid.NewGuid();
                            new_coll.Title = coll_title;
                            _storage.Collections.Add(new_coll);
                            if (my_sc.Collections == null) my_sc.Collections = new System.Collections.ObjectModel.Collection<Collection>();
                            my_sc.Collections.Add(new_coll);
                            _storage.SaveChanges();
                        }
                        string pagesurl = basebatchurl + (string)obj["local_url"];

                        StreamReader reader = new StreamReader(pagesurl);
                        string s = reader.ReadToEnd();
                        reader.Close();
                        JObject jobj = JObject.Parse(s);
                        JArray pagesArray = (JArray)jobj["pages"];

                        //Creating the parent timeline
                        Timeline parent = new Timeline();
                        parent.Id = Guid.NewGuid();
                        parent.Height = null; //todo
                        parent.Regime = "Humanity"; //todo
                        parent.Sequence = 100; //todo
                        parent.Threshold = "8. Origins of modern world"; //todo
                        parent.Title = title;
                        parent.Collection = new_coll;
                        parent.UniqueId = my_timeline_count++;

                        parent.ChildTimelines = new System.Collections.ObjectModel.Collection<Timeline>();
                        //Creating the child exhibits
                        for (int i = 0; i < pagesArray.Count; i++)
                        {
                            Timeline j = new Timeline();
                            j.Id = Guid.NewGuid();
                            j.Title = title + ", Pg" + (i + 1); //Newspaper name,date,pagenumber
                            j.Threshold = "8. Origins of modern world"; //todo
                            j.Regime = "Humanity"; //todo
                            j.Sequence = null; //todo
                            j.FromTimeUnit = "ce"; //to be changed for a different source
                            String dateString = (string)obj["date_issued"];
                            Decimal year = Decimal.Parse(dateString.Substring(0, 4));
                            j.FromMonth = int.Parse(dateString.Substring(5, 2));
                            j.FromDay = int.Parse(dateString.Substring(8, 2));
                            j.FromYear = convertToDecimalYear(j.FromDay, j.FromMonth, year, j.FromTimeUnit);
                            j.ToTimeUnit = j.FromTimeUnit;
                            j.ToDay = j.FromDay;
                            j.ToMonth = j.FromMonth;
                            j.ToYear = j.FromYear;
                            j.Start = j.FromYear;
                            j.End = j.ToYear;
                            j.UniqueId = my_timeline_count++;
                            
                            if (parent.FromYear == 0)
                            {
                                parent.FromTimeUnit = "ce";
                                parent.ToTimeUnit = "ce";
                                parent.FromDay = j.FromDay;
                                parent.FromMonth = j.FromMonth;
                                parent.FromYear = j.FromYear;
                                parent.ToDay = j.ToDay;
                                parent.ToMonth = j.ToMonth;
                                parent.ToYear = j.ToYear;
                                parent.Start = parent.FromYear;
                                parent.End = parent.ToYear;
                            }
                            j.Sequence = 100; //todo
                            j.Height = 100; //null; //todo
                            j.Collection = new_coll;
                            String temp2 = (String)(pagesArray[i])["url"];
                            String URLofData = temp2.Remove(temp2.LastIndexOf(".json")) + "/seq-" + (i + 1);
                            j.Exhibits = new System.Collections.ObjectModel.Collection<Exhibit>();
                            j.Exhibits.Add(createExhibit(j, URLofData, new_coll));
                            parent.ChildTimelines.Add(j);
                            //adding j to db
                            _storage.Timelines.Add(j);
                            if ((my_timeline_count % 10000) == 0)
                                Console.WriteLine("Timelines completed  " + my_timeline_count + "  " + DateTime.Now.ToShortTimeString());
                        }

                        //adding parent to db
                        _storage.Timelines.Add(parent);
                        _storage.SaveChanges();

                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e.Message);
                        Console.WriteLine(e.InnerException);
                        Console.WriteLine(e.StackTrace);
                        continue;
                    }
                }

            }

        }


        //used if creating timeline objects instead of flat json objects
        private Exhibit createExhibit(Timeline j, String URL, Collection col)
        {
            Exhibit e = new Exhibit();
            e.Id = Guid.NewGuid();
            e.Title = j.Title + " - Exhibit"; //todo
            e.Threshold = "8. Origins of modern world"; //todo
            e.Regime = "Humanity";  //todo
            e.TimeUnit = "ce";
            e.Day = j.FromDay;
            e.Month = j.FromMonth;
            e.Year = j.FromYear;
            e.Sequence = 100;
            e.UniqueId = my_exhibit_count++;
            e.Collection = col;
            e.ContentItems = new System.Collections.ObjectModel.Collection<ContentItem>();
            /*
            for (int i = 0; i < 4; i++)
            {
             */
                ContentItem c = new ContentItem();
                c.Id = Guid.NewGuid();
                c.UniqueId = my_contentitem_count++;
                c.Title = j.Title + "- ContentItem";
                c.Threshold = "8. Origins of modern world";
                c.Regime = "Humanity";
                c.TimeUnit = "ce";
                c.Year = j.ToYear;
                c.Order = 1; //todo
                c.HasBibliography = false; //todo
                c.MediaSource = "Library of Congress"; //todo
                c.Attribution = "Library of Congress";  //todo
                c.Collection = col;
            /* note: assuming only pdf is needed */
            /*        
                switch (i)
                {
                    case 0:
                        {
                            c.Caption = j.Title + "- JP2";
                            c.MediaType = "JP2";
                            c.Uri = URL + ".jp2";
                            break;
                        }
                    case 1:
                        {
                            c.Caption = j.Title + "- TXT";
                            c.MediaType = "TXT";
                            c.Uri = URL + "/ocr.txt";
                            break;
                        }
                    case 2:
                        {*/
                            c.Caption = j.Title + "- PDF";
                            c.MediaType = "PDF";
                            c.Uri = URL + ".pdf";
            /*
                            break;
                        }
                    case 3:
                        {
                            c.Caption = j.Title + "- OCR";
                            c.MediaType = "OCR";
                            c.Uri = URL + "/ocr.xml";
                            break;
                        }
             
                }*/
                // Insert into db here
                _storage.ContentItems.Add(c);
                e.ContentItems.Add(c);
            /*
             }
            */
            _storage.Exhibits.Add(e);
            return e;
        }

        /**
       * Taken from Data Migration project
       */
        private static Decimal convertToDecimalYear(int? day, int? month, Decimal? year, string timeUnit)
        {
            Decimal decimalyear = 0;
            if (year.HasValue)
            {
                decimalyear = (Decimal)year;
            }
            else
            {
                return 0; //if the value of the year var is null, return null
            }

            if (timeUnit != null) //if the timeUnit is null - we still calculate decimalyear in the first if of the function and return that value
            {
                if (string.Compare(timeUnit, "ce", true) == 0) //if the timeunit is CE
                {
                    int tempmonth = 1;
                    int tempday = 1;
                    if (month.HasValue && month > 0) //if the month and day values are null, calculating decimalyear with the first day of the year
                    {
                        tempmonth = (int)month;
                        if (day.HasValue && day > 0)
                        {
                            tempday = (int)day;
                        }
                    }
                    DateTime dt = new DateTime((int)decimalyear, tempmonth, tempday);
                    decimalyear = convertToDecimalYear(dt);
                }
                else if (string.Compare(timeUnit, "bce", true) == 0)
                {
                    decimalyear *= -1; //anything that is not CE is in the negative scale. 0 CE = O Decimal Year
                }
                else if (string.Compare(timeUnit, "ka", true) == 0)
                {
                    decimalyear *= -1000;
                }
                else if (string.Compare(timeUnit, "ma", true) == 0)
                {
                    decimalyear *= -1000000;
                }
                else if (string.Compare(timeUnit, "ga", true) == 0)
                {
                    decimalyear *= -1000000000;
                }
                else if (string.Compare(timeUnit, "ta", true) == 0)
                {
                    decimalyear *= -1000000000000;
                }
                else if (string.Compare(timeUnit, "pa", true) == 0)
                {
                    decimalyear *= -1000000000000000;
                }
                else if (string.Compare(timeUnit, "ea", true) == 0)
                {
                    decimalyear *= -1000000000000000000;
                }
                else
                {
                    Console.WriteLine(timeUnit); //was never hit with the current data
                }
            }
            return decimalyear;
        }

        /**
          * Taken from Data Migration project
          */
        private static Decimal convertToDecimalYear(DateTime dateTime)
        {
            Decimal year = dateTime.Year;
            Decimal secondsInThisYear = DateTime.IsLeapYear(dateTime.Year) ? 366 * 24 * 60 * 60 : 365 * 24 * 60 * 60;
            Decimal secondsElapsedSinceYearStart =
                (dateTime.DayOfYear - 1) * 24 * 60 * 60 + dateTime.Hour * 60 * 60 + dateTime.Minute * 60 + dateTime.Second;

            Decimal fractionalYear = secondsElapsedSinceYearStart / secondsInThisYear;

            return year + fractionalYear;
        }

    }
}