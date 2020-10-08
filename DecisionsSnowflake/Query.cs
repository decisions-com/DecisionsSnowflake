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
    public class Query
    {

 public DynamicDataRow[] QuerySnowFlake(string query, string host, string account, string username, string password, string database, string schema, string warehouse, string role)

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

                    conn.ConnectionString = "host=" + host + ";account=" + account + ";role=" + role + ";user=" + username + ";password=" + password + ";db=" + database + ";schema=" + schema + ";warehouse=" + warehouse;

                    conn.Open();



                    IDbCommand cmd = conn.CreateCommand();

                    cmd.CommandText = query;

                    IDataReader reader = cmd.ExecuteReader();

                 



                    // Get the column names in the result set.

                    DataTable schemaTable = reader.GetSchemaTable();



                    // Dynamically generate the outputTable using the columns from above.

                    foreach (DataRow row in schemaTable.Rows)

                    {

                        outputTable.Columns.Add(row["ColumnName"].ToString(), Type.GetType(row["DataType"].ToString()));

                    }



                    int tableColumns = schemaTable.Rows.Count;



                    // Read all resultset rows into outputTable.

                    while (reader.Read())

                    {

                        DataRow row = outputTable.NewRow();





                        for (int i = 0; i < tableColumns; i++)

                        {

                            row[i] = reader[i];

                        }



                        outputTable.Rows.Add(row);

                        dynamicRows.Add(new DynamicDataRow(row));

                    }

                    conn.Close();



                }



                // Create the output result set back to Decisions.

                //Dictionary<string, object> resultData = new Dictionary<string, object>();

                //resultData.Add("Result", LDR.ToArray());

                return dynamicRows.ToArray();

            }

             

            catch (Exception ex2)

            {

                throw;

                

            }

        }
    }
}
