﻿using System;
using System.Linq;
using Hangfire.Mongo.Tests.Utils;
using Hangfire.Storage;
using Xunit;

namespace Hangfire.Mongo.Tests
{
#pragma warning disable 1591
    [Collection("Database")]
    public class MongoStorageFacts
    {
        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionStringIsEmpty()
        {
            var exception = Assert.Throws<ArgumentNullException>(() => new MongoStorage("", "database"));

            Assert.Equal("connectionString", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenDatabaseNameIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(() => new MongoStorage("localhost", null));

            Assert.Equal("databaseName", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenOptionsValueIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(() => new MongoStorage("localhost", "database", null));

            Assert.Equal("options", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void GetMonitoringApi_ReturnsNonNullInstance()
        {
            MongoStorage storage = CreateStorage();
            IMonitoringApi api = storage.GetMonitoringApi();
            Assert.NotNull(api);
        }

        [Fact, CleanDatabase]
        public void GetConnection_ReturnsNonNullInstance()
        {
            MongoStorage storage = CreateStorage();
            using (IStorageConnection connection = storage.GetConnection())
            {
                Assert.NotNull(connection);
            }
        }
        
        private static MongoStorage CreateStorage()
        {
            return new MongoStorage(ConnectionUtils.GetConnectionString(), ConnectionUtils.GetDatabaseName());
        }
    }
#pragma warning restore 1591
}