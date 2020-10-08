using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Runner
{
    class Program
    {
        static void Main(string[] args)
        {
            var sut = new DecisionsSnowflake.QueryRtnJson().QuerySnowFlake("select * from MYTABLE", "epa11700.us-east-1.snowflakecomputing.com", "epa11700", "WEATHEB2", "22", "DEMO_DB", "PUBLIC", "COMPUTE_WH", "SYSADMIN");
                

            var sut2 = new DecisionsSnowflake.Query().QuerySnowFlake("select * from MYTABLE", "epa11700.us-east-1.snowflakecomputing.com", "epa11700", "WEATHEB2", "22", "DEMO_DB", "PUBLIC", "COMPUTE_WH", "SYSADMIN");
               

        }
    }
}
