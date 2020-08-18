using DecisionsFramework.Design.Flow;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SnowFlakeRunner
{
    class Program
    {
        static void Main(string[] args)
        {
            var sut = new Snowflake.SnowflakeIntegration();

            var result = sut.QuerySnowFlake("select * from json_weather_data_view where date_trunc('month',observation_time) = '2018-01-01' limit 10", "https://jba31166.us-east-1.snowflakecomputing.com",
                "jba31166", "jonhallam", "bownem-wyXvix-3kovwy", "Weather", "public");
        }
    }
}
