﻿using System;
using MongoDB.Driver;

namespace Hangfire.Mongo
{
    /// <summary>
    /// Represents extensions to configure MongoDB storage for Hangfire
    /// </summary>
    public static class MongoBootstrapperConfigurationExtensions
    {
        /// <summary>
        /// Configure Hangfire to use MongoDB storage
        /// </summary>
        /// <param name="configuration">Configuration</param>
        /// <param name="connectionString">Connection string for Mongo database, for example 'mongodb://username:passwordY@host:port'</param>
        /// <param name="databaseName">Name of database at Mongo server</param>
        /// <returns></returns>
        public static MongoStorage UseMongoStorage(this IGlobalConfiguration configuration,
            string connectionString,
            string databaseName)
        {
            var storage = new MongoStorage(connectionString, databaseName, new MongoStorageOptions());

            configuration.UseStorage(storage);

            return storage;
        }

        /// <summary>
        /// Configure Hangfire to use MongoDB storage
        /// </summary>
        /// <param name="configuration">Configuration</param>
        /// <param name="connectionString">Connection string for Mongo database, for example 'mongodb://username:passwordY@host:port'</param>
        /// <param name="databaseName">Name of database at Mongo server</param>
        /// <param name="options">Storage options</param>
        /// <returns></returns>
        public static MongoStorage UseMongoStorage(this IGlobalConfiguration configuration,
            string connectionString,
            string databaseName,
            MongoStorageOptions options)
        {
            var storage = new MongoStorage(connectionString, databaseName, options);

            configuration.UseStorage(storage);

            return storage;
        }

        /// <summary>
        /// Configure Hangfire to use MongoDB storage
        /// </summary>
        /// <param name="configuration">Configuration</param>
        /// <param name="mongoClientSettings">Client settings for Mongo</param>
        /// <param name="databaseName">Name of database at Mongo server</param>
        /// <returns></returns>
        public static MongoStorage UseMongoStorage(this IGlobalConfiguration configuration,
            MongoClientSettings mongoClientSettings,
            string databaseName)
        {
            var storage = new MongoStorage(mongoClientSettings, databaseName, new MongoStorageOptions());

            configuration.UseStorage(storage);

            return storage;
        }

        /// <summary>
        /// Configure Hangfire to use MongoDB storage
        /// </summary>
        /// <param name="configuration">Configuration</param>
        /// <param name="mongoClientSettings">Client settings for Mongo</param>
        /// <param name="databaseName">Name of database at Mongo server</param>
        /// <param name="options">Storage options</param>
        /// <returns></returns>
        public static MongoStorage UseMongoStorage(this IGlobalConfiguration configuration,
            MongoClientSettings mongoClientSettings,
            string databaseName,
            MongoStorageOptions options)
        {
            var storage = new MongoStorage(mongoClientSettings, databaseName, options);

            configuration.UseStorage(storage);

            return storage;
        }
    }
}