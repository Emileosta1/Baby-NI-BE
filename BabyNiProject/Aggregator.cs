using System;
using System.Data;
using Vertica.Data;
using Vertica.Data.VerticaClient;

namespace BabyNiProject
{
    public class Aggregator
    {
        private string connectionString;
        private VerticaConnection connection;

        public Aggregator(IConfiguration configuration)
        {
            connectionString = configuration.GetSection("DbSettings:VerticaConnectionString").Value;
            connection = new VerticaConnection(connectionString);
        }

        public void AggregateData()
        {
            connection.Open();

            bool rfInputTableExists = DoesTableExist("RfInput");
            bool radioLinkTableExists = DoesTableExist("RadioLink");

            if (rfInputTableExists && radioLinkTableExists)
            {
                bool hourlyTableExists = DoesTableExist("TRANS_MW_AGG_SLOT_HOURLY");
                bool dailyTableExists = DoesTableExist("TRANS_MW_AGG_SLOT_DAILY");

                if (hourlyTableExists && dailyTableExists)
                {
                    ExecuteExistingTablesQueries();
                }
                else
                {
                    ExecuteCreateTablesQueries();
                }
            }
            else
            {
                Console.WriteLine("Not all required tables (RfInput and RadioLink) exist.");
            }

            connection.Close();
        }

        private bool DoesTableExist(string tableName)
        {
            using (var command = connection.CreateCommand())
            {
                command.CommandText = "SELECT COUNT(*) FROM tables WHERE table_name = :tableName";
                command.Parameters.Add(new VerticaParameter(":tableName", tableName));
                int count = Convert.ToInt32(command.ExecuteScalar());
                return count > 0;
            }
        }

        private void ExecuteExistingTablesQueries()
        {
            string truncate1 = "TRUNCATE TABLE TRANS_MW_AGG_SLOT_HOURLY";
            string truncate2 = "TRUNCATE TABLE TRANS_MW_AGG_SLOT_DAILY";
            string insert1 = "Insert into TRANS_MW_AGG_SLOT_HOURLY select date_trunc('hour',rp.Time) as Time,rf.NeAlias,rf.NeType,rf.DATETIME_KEY, rf.NETWORK_SID, Max(rf.RFInputPower) as RSL_INPUT_POWER,Max(rp.MaxRxLevel) as MaxRxLevel, ABS(MAX(rf.RFInputPower)) - ABS(MAX(rp.MaxRxLevel)) as RSL_DEVIATION from  RfInput rf Inner JOIN RadioLink rp on rf.NETWORK_SID=rp.NETWORK_SID group by 1,2,3,4,5";
            string insert2 = "Insert into TRANS_MW_AGG_SLOT_DAILY select  date_trunc('DAY',rp.Time) as Time, rf.NeAlias, rf.NeType, rf.DATETIME_KEY, rf.NETWORK_SID, Max(rf.RFInputPower) as RSL_INPUT_POWER, Max(rp.MaxRxLevel) as MaxRxLevel, ABS(MAX(rf.RFInputPower)) - ABS(MAX(rp.MaxRxLevel)) as RSL_DEVIATION from  RfInput rf Inner JOIN RadioLink rp on rf.NETWORK_SID=rp.NETWORK_SID group by 1,2,3,4,5";

            Console.WriteLine("Executed queries for existing hourly and daily tables...");

            ExecuteQuery(truncate1);
            ExecuteQuery(truncate2);
            ExecuteQuery(insert1);
            ExecuteQuery(insert2);
        }

        private void ExecuteCreateTablesQueries()
        {
            Console.WriteLine("Executing queries to create hourly and daily tables...");

            string create1 = "CREATE TABLE TRANS_MW_AGG_SLOT_HOURLY (Time TIMESTAMP, NeAlias VARCHAR(30), NeType VARCHAR(30), DATETIME TIMESTAMP, NETWORK_SID INT, RSL_INPUT_POWER FLOAT, MaxRxLevel FLOAT, RSL_Deviation FLOAT);";
            string create2 = "CREATE TABLE TRANS_MW_AGG_SLOT_DAILY (Time TIMESTAMP, NeAlias VARCHAR(30), NeType VARCHAR(30), DATETIME TIMESTAMP, NETWORK_SID INT, RSL_INPUT_POWER FLOAT, MaxRxLevel FLOAT, RSL_Deviation FLOAT);";
            string insert1 = "Insert into TRANS_MW_AGG_SLOT_HOURLY select date_trunc('hour',rp.Time) as Time,rf.NeAlias,rf.NeType,rf.DATETIME_KEY, rf.NETWORK_SID, Max(rf.RFInputPower) as RSL_INPUT_POWER,Max(rp.MaxRxLevel) as MaxRxLevel, ABS(MAX(rf.RFInputPower)) - ABS(MAX(rp.MaxRxLevel)) as RSL_DEVIATION from  RfInput rf Inner JOIN RadioLink rp on rf.NETWORK_SID=rp.NETWORK_SID group by 1,2,3,4,5";
            string insert2 = "Insert into TRANS_MW_AGG_SLOT_DAILY select  date_trunc('DAY',rp.Time) as Time, rf.NeAlias, rf.NeType, rf.DATETIME_KEY, rf.NETWORK_SID, Max(rf.RFInputPower) as RSL_INPUT_POWER, Max(rp.MaxRxLevel) as MaxRxLevel, ABS(MAX(rf.RFInputPower)) - ABS(MAX(rp.MaxRxLevel)) as RSL_DEVIATION from  RfInput rf Inner JOIN RadioLink rp on rf.NETWORK_SID=rp.NETWORK_SID group by 1,2,3,4,5";

            // Execute the create table queries
            ExecuteQuery(create1);
            ExecuteQuery(create2);
            ExecuteQuery(insert1);
            ExecuteQuery(insert2);

        }

        private void ExecuteQuery(string query)
        {
            using (var command = connection.CreateCommand())
            {
                command.CommandText = query;
                command.ExecuteNonQuery();
            }
        }
    }
}
