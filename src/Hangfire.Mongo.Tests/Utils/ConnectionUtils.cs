using System;
using Hangfire.Mongo.Database;

namespace Hangfire.Mongo.Tests.Utils
{
    public static class ConnectionUtils
    {
        private const string DatabaseVariable = "Hangfire_Mongo_DatabaseName";
        private const string ConnectionStringTemplateVariable = "Hangfire_Mongo_ConnectionStringTemplate";

        private const string DefaultDatabaseName = @"Hangfire-Mongo-Tests";
        private const string DefaultConnectionStringTemplate = @"mongodb://localhost";

        public static string GetDatabaseName()
        {
            return Environment.GetEnvironmentVariable(DatabaseVariable) ?? DefaultDatabaseName;
        }

        public static string GetConnectionString()
        {
            return string.Format(GetConnectionStringTemplate(), GetDatabaseName());
        }

        private static string GetConnectionStringTemplate()
        {
            return Environment.GetEnvironmentVariable(ConnectionStringTemplateVariable) ?? DefaultConnectionStringTemplate;
        }

        public static HangfireDbContext CreateConnection()
        {
            var connection = new HangfireDbContext(GetConnectionString(), GetDatabaseName());
            return connection;
        }

        public static MongoStorage CreateStorage()
        {
            var storage = new MongoStorage(GetConnectionString(), GetDatabaseName());
            return storage;
        }
    }
}