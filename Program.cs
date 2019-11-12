using HtmlAgilityPack;
using Microsoft.Spark.Sql;
using System;
using System.IO;
using System.Linq;
using System.Net.Http;

namespace meduim_words
{
    class Program
    {
        static void Main(string[] args)
        {
            var spark = SparkSession
               .Builder()
               .AppName("word_count_sample")
               .GetOrCreate();
            var conf = spark.Conf();

            HttpClient client = new HttpClient();

            var content  = client.GetAsync("https://raw.githubusercontent.com/AMustapha/meduim_words/master/storie.txt")
                                 .Result.Content.ReadAsStringAsync().Result;
            File.WriteAllText("../../../storie.txt",content);

            DataFrame dataFrame = spark.Read().Text("storie.txt");
            var words = dataFrame
                .Select(Functions.Split(Functions.Col("value"), " ").Alias("words"))
                .Select(Functions.Explode(Functions.Col("words"))
                .Alias("word"))
                .GroupBy("word")
                .Count()
                .OrderBy(Functions.Col("count").Desc());

            // Show results
            words.Show();

        }
    }
}
