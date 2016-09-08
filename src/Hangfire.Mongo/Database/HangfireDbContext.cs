using System;
using Hangfire.Mongo.Dto;
using MongoDB.Bson;
using MongoDB.Driver;
using System.Linq.Expressions;

namespace Hangfire.Mongo.Database
{
	/// <summary>
	/// Represents Mongo database context for Hangfire
	/// </summary>
	[System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1063:ImplementIDisposableCorrectly")]
	public class HangfireDbContext : IDisposable
    {
        private const int RequiredSchemaVersion = 4;

        private readonly string _prefix;

        internal IMongoDatabase Database { get; private set; }

        /// <summary>
        /// Constructs context with connection string and database name
        /// </summary>
        /// <param name="connectionString">Connection string for Mongo database</param>
        /// <param name="databaseName">Database name</param>
        /// <param name="prefix">Collections prefix</param>
        public HangfireDbContext(string connectionString, string databaseName, string prefix = "hangfire")
        {
            _prefix = prefix;

            MongoClient client = new MongoClient(connectionString);

            Database = client.GetDatabase(databaseName);

            ConnectionId = Guid.NewGuid().ToString();
        }

		/// <summary>
		/// Constructs context with Mongo client settings and database name
		/// </summary>
		/// <param name="mongoClientSettings">Client settings for MongoDB</param>
		/// <param name="databaseName">Database name</param>
		/// <param name="prefix">Collections prefix</param>
		public HangfireDbContext(MongoClientSettings mongoClientSettings, string databaseName, string prefix = "hangfire")
		{
			_prefix = prefix;

			MongoClient client = new MongoClient(mongoClientSettings);

			Database = client.GetDatabase(databaseName);

			ConnectionId = Guid.NewGuid().ToString();
		}

        /// <summary>
        /// Constructs context with existing Mongo database connection
        /// </summary>
        /// <param name="database">Database connection</param>
        public HangfireDbContext(IMongoDatabase database)
        {
            Database = database;
            ConnectionId = Guid.NewGuid().ToString();
        }

        /// <summary>
        /// Mongo database connection identifier
        /// </summary>
        public string ConnectionId { get; private set; }

        /// <summary>
        /// Reference to collection which contains identifiers
        /// </summary>
        internal virtual IMongoCollection<IdentifierDto> Identifiers
        {
	        get { return Database.GetCollection<IdentifierDto>(_prefix + "_identifiers"); }
        }

	    /// <summary>
        /// Reference to collection which contains distributed locks
        /// </summary>
        internal virtual IMongoCollection<DistributedLockDto> DistributedLock
	    {
		    get
            {
                return Database.GetCollection<DistributedLockDto>(_prefix + ".locks")
                               .WithWriteConcern(WriteConcern.WMajority);
            }
	    }

	    /// <summary>
        /// Reference to collection which contains counters
        /// </summary>
        internal virtual IMongoCollection<CounterDto> Counter
	    {
		    get { return Database.GetCollection<CounterDto>(_prefix + ".counter"); }
	    }

	    /// <summary>
        /// Reference to collection which contains aggregated counters
        /// </summary>
        internal virtual IMongoCollection<AggregatedCounterDto> AggregatedCounter
	    {
		    get { return Database.GetCollection<AggregatedCounterDto>(_prefix + ".aggregatedcounter"); }
	    }

	    /// <summary>
        /// Reference to collection which contains hashes
        /// </summary>
        internal virtual IMongoCollection<HashDto> Hash
	    {
		    get { return Database.GetCollection<HashDto>(_prefix + ".hash"); }
	    }

	    /// <summary>
        /// Reference to collection which contains jobs
        /// </summary>
        internal virtual IMongoCollection<JobDto> Job
	    {
		    get { return Database.GetCollection<JobDto>(_prefix + ".job"); }
	    }

	    /// <summary>
        /// Reference to collection which contains jobs parameters
        /// </summary>
        internal virtual IMongoCollection<JobParameterDto> JobParameter
	    {
		    get { return Database.GetCollection<JobParameterDto>(_prefix + ".jobParameter"); }
	    }

	    /// <summary>
        /// Reference to collection which contains jobs queues
        /// </summary>
        internal virtual IMongoCollection<JobQueueDto> JobQueue
	    {
		    get { return Database.GetCollection<JobQueueDto>(_prefix + ".jobQueue"); }
	    }

	    /// <summary>
        /// Reference to collection which contains lists
        /// </summary>
        internal virtual IMongoCollection<ListDto> List
	    {
		    get { return Database.GetCollection<ListDto>(_prefix + ".list"); }
	    }

	    /// <summary>
        /// Reference to collection which contains schemas
        /// </summary>
        internal virtual IMongoCollection<SchemaDto> Schema
	    {
		    get { return Database.GetCollection<SchemaDto>(_prefix + ".schema"); }
	    }

	    /// <summary>
        /// Reference to collection which contains servers information
        /// </summary>
        internal virtual IMongoCollection<ServerDto> Server
	    {
		    get { return Database.GetCollection<ServerDto>(_prefix + ".server"); }
	    }

	    /// <summary>
        /// Reference to collection which contains sets
        /// </summary>
        internal virtual IMongoCollection<SetDto> Set
	    {
		    get { return Database.GetCollection<SetDto>(_prefix + ".set"); }
	    }

	    /// <summary>
        /// Reference to collection which contains states
        /// </summary>
        internal virtual IMongoCollection<StateDto> State
	    {
		    get { return Database.GetCollection<StateDto>(_prefix + ".state"); }
	    }

	    /// <summary>
        /// Initializes intial collections schema for Hangfire
        /// </summary>
        public void Init()
        {
            SchemaDto schema = Schema.Find(new BsonDocument()).FirstOrDefault();

            if (schema != null)
            {
                if (RequiredSchemaVersion > schema.Version)
                {
                    Schema.DeleteMany(new BsonDocument());
                    Schema.InsertOne(new SchemaDto { Version = RequiredSchemaVersion });
                }
                else if (RequiredSchemaVersion < schema.Version)
                {
                    throw new InvalidOperationException(string.Format(
                        "HangFire current database schema version {0} is newer than the configured MongoStorage schema version {1}. " +
                        "Please update to the latest HangFire.SqlServer NuGet package.", schema.Version, RequiredSchemaVersion));
                }
            }
            else
            {
	            Schema.InsertOne(new SchemaDto {Version = RequiredSchemaVersion});
            }

            // create TTL indexes

            CreateTtlIndex(AggregatedCounter, _ => _.ExpireAt);
            CreateTtlIndex(Counter, _ => _.ExpireAt);
            CreateTtlIndex(DistributedLock, _ => _.ExpireAt);
            CreateTtlIndex(Hash, _ => _.ExpireAt);
            CreateTtlIndex(Job, _ => _.ExpireAt);
            CreateTtlIndex(JobParameter, _ => _.ExpireAt);
            CreateTtlIndex(List, _ => _.ExpireAt);
            CreateTtlIndex(Set, _ => _.ExpireAt);
            CreateTtlIndex(State, _ => _.ExpireAt);
        }

        private void CreateTtlIndex<TEntity>(IMongoCollection<TEntity> collection, Expression<Func<TEntity, object>> field)
        {
            collection.Indexes.CreateOne(Builders<TEntity>.IndexKeys.Ascending(field), new CreateIndexOptions()
            {
                Name = "ix_ttl",
                ExpireAfter = TimeSpan.Zero,
                Background = true
            });
        }

		/// <summary>
		/// Disposes the object
		/// </summary>
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1063:ImplementIDisposableCorrectly", 
			Justification="Dispose should only implement finalizer if owning an unmanaged resource")]
		public void Dispose()
        {
        }
    }
}