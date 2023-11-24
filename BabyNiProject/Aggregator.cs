using System;
using System.Data;
using Vertica.Data;
using Vertica.Data.VerticaClient;

namespace BabyNiProject
{
    public class Aggregator
    {
        private string connectionString; // Your Vertica database connection string
        private VerticaConnection connection; // The connection object

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
                    // Execute queries for existing hourly and daily tables
                    ExecuteExistingTablesQueries();
                }
                else
                {
                    // Execute queries for creating hourly and daily tables
                    ExecuteCreateTablesQueries();

                    // Additional processing steps if needed
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

            // Add queries to execute when hourly and daily tables exist
            Console.WriteLine("Executing queries for existing hourly and daily tables...");

            ExecuteQuery(truncate1);
            ExecuteQuery(truncate2);
            ExecuteQuery(insert1);
            ExecuteQuery(insert2);
        }

        private void ExecuteCreateTablesQueries()
        {
            // Add queries to execute when hourly and daily tables need to be created
            Console.WriteLine("Executing queries to create hourly and daily tables...");

            // Example queries (modify as needed)
            string create1 = "CREATE TABLE TRANS_MW_AGG_SLOT_HOURLY (Time TIMESTAMP, NeAlias VARCHAR(30), NeType VARCHAR(30), DATETIME TIMESTAMP, NETWORK_SID INT, RSL_INPUT_POWER FLOAT, MaxRxLevel FLOAT, RSL_Deviation FLOAT);";
            string create2 = "CREATE TABLE TRANS_MW_AGG_SLOT_DAILY (Time TIMESTAMP, NeAlias VARCHAR(30), NeType VARCHAR(30), DATETIME TIMESTAMP, NETWORK_SID INT, RSL_INPUT_POWER FLOAT, MaxRxLevel FLOAT, RSL_Deviation FLOAT);";
            string insert1 = "Insert into TRANS_MW_AGG_SLOT_HOURLY select date_trunc('hour',rp.Time) as Time,rf.NeAlias,rf.NeType,rf.DATETIME_KEY, rf.NETWORK_SID, Max(rf.RFInputPower) as RSL_INPUT_POWER,Max(rp.MaxRxLevel) as MaxRxLevel, ABS(MAX(rf.RFInputPower)) - ABS(MAX(rp.MaxRxLevel)) as RSL_DEVIATION from  RfInput rf Inner JOIN RadioLink rp on rf.NETWORK_SID=rp.NETWORK_SID group by 1,2,3,4,5";
            string insert2 = "Insert into TRANS_MW_AGG_SLOT_DAILY select  date_trunc('DAY',rp.Time) as Time, rf.NeAlias, rf.NeType, rf.DATETIME_KEY, rf.NETWORK_SID, Max(rf.RFInputPower) as RSL_INPUT_POWER, Max(rp.MaxRxLevel) as MaxRxLevel, ABS(MAX(rf.RFInputPower)) - ABS(MAX(rp.MaxRxLevel)) as RSL_DEVIATION from  RfInput rf Inner JOIN RadioLink rp on rf.NETWORK_SID=rp.NETWORK_SID group by 1,2,3,4,5";

            // Execute the create table queries
            ExecuteQuery(create1);
            ExecuteQuery(create2);
            ExecuteQuery(insert1);
            ExecuteQuery(insert2);

            // Additional processing steps if needed
        }

        private void ExecuteQuery(string query)
        {
            // Example method to execute queries, modify based on your database library
            using (var command = connection.CreateCommand())
            {
                command.CommandText = query;
                command.ExecuteNonQuery();
            }
        }
    }
}
