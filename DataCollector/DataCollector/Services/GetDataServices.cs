using NIGetData.models;
using System;
using System.Collections.Generic;
using Vertica.Data.VerticaClient;

namespace NIGetData.services
{
    public class GetDataService
    {
        public List<DataModel> GetData(BodyDataModel bodyDataModel)
        {
            // Build Vertica connection string
            var connectionStringBuilder = new VerticaConnectionStringBuilder
            {
                Host = "10.10.4.231",
                Database = "test",
                Port = 5433,
                User = "bootcamp1",
                Password = "bootcamp12023"
            };

            
            string query = $@"
SELECT 
    {bodyDataModel.Ne} as Ne,
    Time,
    Max(RSL_INPUT_POWER) as RSL_INPUT_POWER,
    Max(MaxRxLevel) as MaxRxLevel,
    ABS(Max(RSL_INPUT_POWER)) - ABS(Max(MaxRxLevel)) as RSL_Deviation
FROM  
    {bodyDataModel.table}
WHERE
    Time BETWEEN '{bodyDataModel.timeStart}' AND '{bodyDataModel.timeEnd}'
GROUP BY 
    Time, {bodyDataModel.Ne}
ORDER BY
    Time;";

            using (var connection = new VerticaConnection(connectionStringBuilder.ToString()))
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = query;

                    using (var dataReader = command.ExecuteReader())
                    {
                        return GetDataFromReader(dataReader);
                    }
                }
            }
        }

        private List<DataModel> GetDataFromReader(VerticaDataReader dataReader)
        {
            var result = new List<DataModel>();

            while (dataReader.Read())
            {
                var rowResult = new DataModel
                {
                    Time = dataReader.GetDateTime(dataReader.GetOrdinal("Time")),
                    Ne = dataReader.GetString(dataReader.GetOrdinal("Ne")),
                    RSL_INPUT_POWER = dataReader.GetFloat(dataReader.GetOrdinal("RSL_INPUT_POWER")),
                    MaxRxLevel = dataReader.GetFloat(dataReader.GetOrdinal("MaxRxLevel")),
                    RSL_Deviation = dataReader.GetFloat(dataReader.GetOrdinal("RSL_Deviation")),
                };

                result.Add(rowResult);
            }

            return result;
        }
    }
}
