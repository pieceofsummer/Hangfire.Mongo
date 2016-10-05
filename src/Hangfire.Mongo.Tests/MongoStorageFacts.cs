using System;
using Hangfire.Mongo.Tests.Utils;
using Hangfire.Storage;
using Xunit;

namespace Hangfire.Mongo.Tests
{
    [Collection("Database")]
    public class MongoStorageFacts
    {
        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionStringIsEmpty()
        {
            Assert.Throws<ArgumentNullException>("connectionString",
                () => new MongoStorage("", "database"));
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenDatabaseNameIsNull()
        {
            Assert.Throws<ArgumentNullException>("databaseName",
                () => new MongoStorage("localhost", null));
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenOptionsIsNull()
        {
            Assert.Throws<ArgumentNullException>("options",
                () => new MongoStorage("localhost", "database", null));
        }

        [Fact, CleanDatabase]
        public void GetMonitoringApi_ReturnsNonNullInstance()
        {
            using (MongoStorage storage = ConnectionUtils.CreateStorage())
            {
                IMonitoringApi api = storage.GetMonitoringApi();
                Assert.NotNull(api);
            }
        }

        [Fact, CleanDatabase]
        public void GetConnection_ReturnsNonNullInstance()
        {
            using (MongoStorage storage = ConnectionUtils.CreateStorage())
            using (IStorageConnection connection = storage.GetConnection())
            {
                Assert.NotNull(connection);
            }
        }
        
    }
}