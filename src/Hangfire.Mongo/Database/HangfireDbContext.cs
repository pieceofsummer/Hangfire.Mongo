using System;
using Hangfire.Mongo.Dto;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Core.Configuration;
using MongoDB.Driver.Core.Events;
using System.Diagnostics;
using System.Linq;
using Hangfire.Logging;
using System.Collections.Concurrent;

namespace Hangfire.Mongo.Database
{
    /// <summary>
    /// Represents Mongo database context for Hangfire
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1063:ImplementIDisposableCorrectly")]
	public class HangfireDbContext : IDisposable
    {
        private static readonly ILog _logger = LogProvider.For<HangfireDbContext>();

        private const int RequiredSchemaVersion = 6;
        
        private readonly string _prefix;

        internal IMongoDatabase Database { get; private set; }

        /// <summary>
        /// Constructs context with connection string and database name
        /// </summary>
        /// <param name="connectionString">Connection string for Mongo database</param>
        /// <param name="databaseName">Database name</param>
        /// <param name="prefix">Collections prefix</param>
        public HangfireDbContext(string connectionString, string databaseName, string prefix = "hangfire")
            : this(MongoClientSettings.FromUrl(MongoUrl.Create(connectionString)), databaseName, prefix)
        {
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

            mongoClientSettings.ClusterConfigurator = ClasterBuilderCallback;

			MongoClient client = new MongoClient(mongoClientSettings);
            
            Database = client.GetDatabase(databaseName);

			ConnectionId = Guid.NewGuid().ToString();
            
            Init();
		}
        
        private void ClasterBuilderCallback(ClusterBuilder builder)
        {
            builder.Subscribe<CommandStartedEvent>(OnCommandStarted);
            builder.Subscribe<CommandSucceededEvent>(OnCommandSucceeded);
            builder.Subscribe<CommandFailedEvent>(OnCommandFailed);
        }
        
        private void OnCommandStarted(CommandStartedEvent e)
        {
            if (e.OperationId.HasValue)
            {
                _logger.TraceFormat("< [{0}] {1}: {2}", e.OperationId, e.CommandName, e.Command);
            }
        }

        private void OnCommandSucceeded(CommandSucceededEvent e)
        {
            // capture local time as soon as reply is received to minimize errors
            var now = DateTime.UtcNow;

            if (e.OperationId.HasValue)
            {
                _logger.TraceFormat("> [{0}] {1} SUCC in {2}", e.OperationId, e.CommandName, e.Duration);
            }
            
            if (e.CommandName == "isMaster")
            {
                UpdateServerTimeFromReply(now, e.Reply);
            }
            else if (e.CommandName == "buildInfo")
            {
                UpdateServerInfoFromReply(now, e.Reply);
            }
        }

        private void OnCommandFailed(CommandFailedEvent e)
        {
            if (e.OperationId.HasValue)
            {
                _logger.TraceFormat("> [{0}] {1} FAIL in {2}", e.OperationId, e.CommandName, e.Duration);
            }
        }

        #region ServerVersion tracking

        private DateTime? _serverInfoReceived = null;
        private Version _serverVersion = null;
        
        private void UpdateServerInfoFromReply(DateTime now, BsonDocument reply)
        {
            BsonValue value;

            if (reply.TryGetValue("versionArray", out value))
            {
                var versionArray = value.AsBsonArray;

                var revision = versionArray[3].AsInt32;

                _serverVersion = new Version(
                    versionArray[0].AsInt32,    // major
                    versionArray[1].AsInt32,    // minor
                    versionArray[2].AsInt32);   // revision

                // do not log too often
                Debug.WriteLineIf(
                    !_serverTimeReceived.HasValue || (now - _serverTimeReceived.Value) > TimeSpan.FromSeconds(5),
                    $"[{now}] Updated version from server: {_serverVersion}");

                _serverInfoReceived = now;
            }
        }
        
        /// <summary>
        /// Returns server version, as reported by buildInfo()
        /// </summary>
        /// <returns></returns>
        public Version GetServerVersion()
        {
            if (_serverVersion == null)
            {
                // not received version yet
                // in reality this should never happen, because server report 'buildInfo' often
                // but let's heep it be here, in case command callbacks were disabled

                var reply = Database.RunCommand<BsonDocument>(new BsonDocument("buildInfo", 1));

                if (_serverVersion != null)
                {
                    // Command handler has already updated info, updating it once again is pointless
                }
                else
                {
                    // No command handler present for some reason, perform an update
                    UpdateServerInfoFromReply(DateTime.UtcNow, reply);
                }
            }

            return _serverVersion;
        }

        #endregion

        #region ServerTime tracking

        private DateTime? _serverTimeReceived = null;
        private DateTime _serverTime;

        private void UpdateServerTimeFromReply(DateTime now, BsonDocument reply)
        {
            BsonValue value;
            if (reply.TryGetValue("localTime", out value))
            {
                _serverTime = value.ToUniversalTime();

                // do not log too often
                Debug.WriteLineIf(
                    !_serverTimeReceived.HasValue || (now - _serverTimeReceived.Value) > TimeSpan.FromSeconds(5),
                    $"[{now}] Updated time from server: {_serverTime}");
                
                _serverTimeReceived = now;
            }
        }

        /// <summary>
        /// Returns local time on server, as reported by isMaster()
        /// </summary>
        [DebuggerStepThrough]
        public DateTime GetServerTimeUtc()
        {
            var requestTime = DateTime.UtcNow;

            if (!_serverTimeReceived.HasValue || 
                requestTime - _serverTimeReceived.Value > TimeSpan.FromMinutes(10))
            {
                // not received time yet, or value is too old
                // in reality this should never happen, because server reports 'isMaster' often
                // but let's heep it be here, in case command callbacks were disabled

                Debug.WriteLine("Forcing time request from server");

                var reply = Database.RunCommand<BsonDocument>(new BsonDocument("isMaster", 1));
                
                if (_serverTimeReceived.HasValue && 
                    _serverTimeReceived.Value > requestTime)
                {
                    // Command handler has already updated time, updating it once again will only hurt precision
                }
                else
                {
                    // No command handler present for some reason, perform an update
                    UpdateServerTimeFromReply(DateTime.UtcNow, reply);
                }

                // update now, so we calculate gap correctly
                requestTime = DateTime.UtcNow;
            }

            return _serverTime + (requestTime - _serverTimeReceived.Value);
        }

        #endregion

        /// <summary>
        /// Mongo database connection identifier
        /// </summary>
        public string ConnectionId { get; private set; }

        /// <summary>
        /// Reference to collection which contains distributed locks
        /// </summary>
        internal virtual IMongoCollection<DistributedLockDto> DistributedLock
            => Database.GetCollection<DistributedLockDto>(_prefix + ".locks")
                               .WithWriteConcern(WriteConcern.WMajority);

	    /// <summary>
        /// Reference to collection which contains counters
        /// </summary>
        internal virtual IMongoCollection<CounterDto> Counter
            => Database.GetCollection<CounterDto>(_prefix + ".counter");

	    /// <summary>
        /// Reference to collection which contains aggregated counters
        /// </summary>
        internal virtual IMongoCollection<AggregatedCounterDto> AggregatedCounter
            => Database.GetCollection<AggregatedCounterDto>(_prefix + ".aggregatedcounter");

	    /// <summary>
        /// Reference to collection which contains hashes
        /// </summary>
        internal virtual IMongoCollection<HashDto> Hash
            => Database.GetCollection<HashDto>(_prefix + ".hash");

	    /// <summary>
        /// Reference to collection which contains jobs
        /// </summary>
        internal virtual IMongoCollection<JobDto> Job
            => Database.GetCollection<JobDto>(_prefix + ".job");

	    /// <summary>
        /// Reference to collection which contains jobs parameters
        /// </summary>
        internal virtual IMongoCollection<JobParameterDto> JobParameter
            => Database.GetCollection<JobParameterDto>(_prefix + ".jobParameter");
        
	    /// <summary>
        /// Reference to collection which contains lists
        /// </summary>
        internal virtual IMongoCollection<ListDto> List
            => Database.GetCollection<ListDto>(_prefix + ".list");

	    /// <summary>
        /// Reference to collection which contains schemas
        /// </summary>
        internal virtual IMongoCollection<SchemaDto> Schema
            => Database.GetCollection<SchemaDto>(_prefix + ".schema");

	    /// <summary>
        /// Reference to collection which contains servers information
        /// </summary>
        internal virtual IMongoCollection<ServerDto> Server
            => Database.GetCollection<ServerDto>(_prefix + ".server");

	    /// <summary>
        /// Reference to collection which contains sets
        /// </summary>
        internal virtual IMongoCollection<SetDto> Set
            => Database.GetCollection<SetDto>(_prefix + ".set");

	    /// <summary>
        /// Reference to collection which contains states
        /// </summary>
        internal virtual IMongoCollection<StateDto> State
            => Database.GetCollection<StateDto>(_prefix + ".state");

	    /// <summary>
        /// Initializes intial collections schema for Hangfire
        /// </summary>
        public void Init()
        {
            Schema.UpdateOne(
                Builders<SchemaDto>.Filter.Eq(_ => _.Version, RequiredSchemaVersion),
                Builders<SchemaDto>.Update.Unset("x"), // fake update operation
                new UpdateOptions() { IsUpsert = true });

            var schema = Schema.Aggregate()
                .Group(k => 0, g => new { _id = g.Max(_ => _.Version) }).As<SchemaDto>()
                .Out(Schema.CollectionNamespace.CollectionName)
                .Single();

            if (RequiredSchemaVersion < schema.Version)
            {
                throw new InvalidOperationException(string.Format(
                    "Current database schema version ({0}) is newer than supported by this package ({1}). " +
                    "Please update to the latest version of package.", schema.Version, RequiredSchemaVersion));
            }

            // create indexes

            CreateIndex(Counter, "ix_key", ix => ix.Ascending(_ => _.Key));
            CreateIndex(Hash, "ix_key_field", ix => ix.Ascending(_ => _.Key).Ascending(_ => _.Field), unique: true);
            CreateIndex(Job, "ix_queue_fetchedAt", ix => ix.Ascending(_ => _.Queue).Ascending(_ => _.FetchedAt), sparse: true);
            CreateIndex(Job, "ix_stateName_id", ix => ix.Ascending(_ => _.StateName).Descending(_ => _.Id));
            CreateIndex(JobParameter, "ix_jobId_name", ix => ix.Descending(_ => _.JobId).Ascending(_ => _.Name), unique: true);
            CreateIndex(List, "ix_key", ix => ix.Ascending(_ => _.Key));
            CreateIndex(Set, "ix_key", ix => ix.Ascending(_ => _.Key));
            CreateIndex(State, "ix_jobId_createdAt", ix => ix.Descending(_ => _.JobId).Descending(_ => _.CreatedAt));

            // create TTL indexes

            CreateIndex(AggregatedCounter, "ix_ttl", ix => ix.Ascending(_ => _.ExpireAt), sparse: true, expireAfter: TimeSpan.Zero);
            CreateIndex(Counter, "ix_ttl", ix => ix.Ascending(_ => _.ExpireAt), sparse: true, expireAfter: TimeSpan.Zero);
            CreateIndex(DistributedLock, "ix_ttl", ix => ix.Ascending(_ => _.ExpireAt), sparse: true, expireAfter: TimeSpan.Zero);
            CreateIndex(Hash, "ix_ttl", ix => ix.Ascending(_ => _.ExpireAt), sparse: true, expireAfter: TimeSpan.Zero);
            CreateIndex(Job, "ix_ttl", ix => ix.Ascending(_ => _.ExpireAt), sparse: true, expireAfter: TimeSpan.Zero);
            CreateIndex(JobParameter, "ix_ttl", ix => ix.Ascending(_ => _.ExpireAt), sparse: true, expireAfter: TimeSpan.Zero);
            CreateIndex(List, "ix_ttl", ix => ix.Ascending(_ => _.ExpireAt), sparse: true, expireAfter: TimeSpan.Zero);
            CreateIndex(Set, "ix_ttl", ix => ix.Ascending(_ => _.ExpireAt), sparse: true, expireAfter: TimeSpan.Zero);
            CreateIndex(State, "ix_ttl", ix => ix.Ascending(_ => _.ExpireAt), sparse: true, expireAfter: TimeSpan.Zero);
        }

        private void CreateIndex<TEntity>(IMongoCollection<TEntity> collection, string name, Func<IndexKeysDefinitionBuilder<TEntity>, IndexKeysDefinition<TEntity>> configure, 
                                          bool? unique = null, bool? sparse = null, TimeSpan? expireAfter = null)
        {
            var keys = configure(Builders<TEntity>.IndexKeys);

            var options = new CreateIndexOptions()
            {
                Name = name,
                Background = true,
                Unique = unique,
                Sparse = sparse,
                ExpireAfter = expireAfter
            };

            try
            {
                collection.Indexes.CreateOne(keys, options);
            }
            catch (MongoCommandException ex) when (ex.Code == 85)
            {
                // index exists with different options
                collection.Indexes.DropOne(name);
                collection.Indexes.CreateOne(keys, options);
            }
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