using DecisionsFramework.Design.Flow.CoreSteps.StandardSteps;
using Newtonsoft.Json;
using Snowflake.Data.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DecisionsSnowflake
{
    public class QueryRtnJson
    {
        private IEnumerable<Dictionary<string, object>> rows;

        public string QuerySnowFlake(string query, string host, string account, string username, string password, string database, string schema, string warehouse)

        {

            try

            {

                // Defined for use as the output handler.

                DataTable outputTable = new DataTable();

                List<DynamicDataRow> dynamicRows = new List<DynamicDataRow>();



                using (IDbConnection conn = new SnowflakeDbConnection())

                {

                    if (host.StartsWith("http", StringComparison.OrdinalIgnoreCase))

                    {

                        host = host.Split(':').Last();

                        host = host.Replace("//", string.Empty);

                    }

                    // Create the connection string and connect to Snowflake instance.

                    conn.ConnectionString = "host=" + host + ";account=" + account + ";user=" + username + ";password=" + password + ";db=" + database + ";schema=" + schema + ";warehouse=" + warehouse;

                    conn.Open();



                    IDbCommand cmd = conn.CreateCommand();

                    cmd.CommandText = query;

                    IDataReader reader = cmd.ExecuteReader();


                    rows = ConvertToDictionary(reader);
                    


                    conn.Close();



                }



                // Create the output result set back to Decisions.

                //Dictionary<string, object> resultData = new Dictionary<string, object>();

                //resultData.Add("Result", LDR.ToArray());

                return JsonConvert.SerializeObject(rows);

            }



            catch (Exception ex2)

            {

                throw;



            }

        }
        private IEnumerable<Dictionary<string, object>> ConvertToDictionary(IDataReader reader)
        {
            var columns = new List<string>();
            var rows = new List<Dictionary<string, object>>();

            for (var i = 0; i < reader.FieldCount; i++)
            {
                columns.Add(reader.GetName(i));
            }

            while (reader.Read())
            {
                rows.Add(columns.ToDictionary(column => column, column => reader[column]));
            }
            return rows;
        }
    }
}
