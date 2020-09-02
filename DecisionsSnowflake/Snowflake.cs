using System;

using System.Collections.Generic;

using System.Data;

using System.Linq;

using System.Runtime.InteropServices.WindowsRuntime;

using DecisionsFramework.Design.ConfigurationStorage.Attributes;

using DecisionsFramework.Design.Flow;

using DecisionsFramework.Design.Flow.CoreSteps;

using DecisionsFramework.Design.Flow.CoreSteps.StandardSteps;

using DecisionsFramework.Design.Flow.Mapping;

using DecisionsFramework.ServiceLayer.Services.ContextData;

using Snowflake.Data.Client;



namespace Snowflake

{

    [AutoRegisterStep("Run Query", "Integration/Snowflake")]

    [Writable]

    public class SnowflakeIntegration : BaseFlowAwareStep, ISyncStep, IDataConsumer, IDataProducer

    {

        public DataDescription[] InputData

        {

            get { 

                return new DataDescription[] 

                    {

                        new DataDescription(new DecisionsNativeType(typeof(string)), "Host"),

                        new DataDescription(new DecisionsNativeType(typeof(string)), "Account"),

                        new DataDescription(new DecisionsNativeType(typeof(string)), "Username"),

                        new DataDescription(new DecisionsNativeType(typeof(string)), "Password"),

                        new DataDescription(new DecisionsNativeType(typeof(string)), "Database"),

                        new DataDescription(new DecisionsNativeType(typeof(string)), "Schema"),

                        new DataDescription(new DecisionsNativeType(typeof(string)), "Query"),

                        new DataDescription(new DecisionsNativeType(typeof(string)), "Warehouse")

                    };

            

                }

        }

        public override OutcomeScenarioData[] OutcomeScenarios

        {

            get {

                return new OutcomeScenarioData[]

                  {

                      new OutcomeScenarioData("Done", new DataDescription[] { new DataDescription(new DecisionsNativeType(typeof(DynamicDataRow)), "Result", true, true, false) })

                  };

                }

        }



        public ResultData Run(StepStartData data)

        {

            string query = (string)data.Data["Query"];

            string host = (string)data.Data["Host"];

            string account = (string)data.Data["Account"];

            string username = (string)data.Data["Username"];

            string password = (string)data.Data["Password"];

            string database = (string)data.Data["Database"];

            string schema = (string)data.Data["Schema"];

            string warehouse = (string)data.Data["Warehouse"];

            var result = QuerySnowFlake(query, host, account, username, password, database, schema, warehouse);

            return new ResultData("Done", new DataPair[] { new DataPair("Result", result) });

        }

         





        public DynamicDataRow[] QuerySnowFlake(string query, string host, string account, string username, string password, string database, string schema, string warehouse)

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

