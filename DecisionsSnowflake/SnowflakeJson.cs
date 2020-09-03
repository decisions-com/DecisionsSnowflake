using System;

using System.Collections.Generic;

using System.Data;

using System.Linq;

using System.Runtime.InteropServices.WindowsRuntime;
using AngleSharp.Common;
using DecisionsFramework.Design.ConfigurationStorage.Attributes;

using DecisionsFramework.Design.Flow;

using DecisionsFramework.Design.Flow.CoreSteps;

using DecisionsFramework.Design.Flow.CoreSteps.StandardSteps;

using DecisionsFramework.Design.Flow.Mapping;

using DecisionsFramework.ServiceLayer.Services.ContextData;
using DecisionsSnowflake;
using Newtonsoft.Json;
using Snowflake.Data.Client;



namespace SnowflakeJson

{

    [AutoRegisterStep("Run Query Return Json", "Integration/Snowflake")]

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

                      new OutcomeScenarioData("Done", new DataDescription[] { new DataDescription(new DecisionsNativeType(typeof(string)), "Result", false, true, false) })

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

            var result = new DecisionsSnowflake.QueryRtnJson().QuerySnowFlake(query, host, account, username, password, database, schema, warehouse);

            return new ResultData("Done", new DataPair[] { new DataPair("Result", result) });

        }

         





       




       

    }

}

