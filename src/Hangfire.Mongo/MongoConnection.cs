using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Hangfire.Common;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.DistributedLock;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.PersistentJobQueue;
using Hangfire.Server;
using Hangfire.Storage;
using MongoDB.Driver;

namespace Hangfire.Mongo
{
    /// <summary>
    ///     MongoDB database connection for Hangfire
    /// </summary>
    internal class MongoConnection : JobStorageConnection
    {
        private readonly MongoStorageOptions _options;

        private readonly PersistentJobQueueProviderCollection _queueProviders;

        private static readonly TimeSpan NoTtl = TimeSpan.FromSeconds(-1);

        public MongoConnection(HangfireDbContext database, PersistentJobQueueProviderCollection queueProviders)
            : this(database, new MongoStorageOptions(), queueProviders)
        {
        }

        public MongoConnection(HangfireDbContext database, MongoStorageOptions options, PersistentJobQueueProviderCollection queueProviders)
        {
            if (database == null)
                throw new ArgumentNullException(nameof(database));

            if (queueProviders == null)
                throw new ArgumentNullException(nameof(queueProviders));

            if (options == null)
                throw new ArgumentNullException(nameof(options));

            Database = database;
            _options = options;
            _queueProviders = queueProviders;
        }

        public HangfireDbContext Database { get; }
        
        public override IWriteOnlyTransaction CreateWriteTransaction()
        {
            return new MongoWriteOnlyTransaction(Database, _queueProviders);
        }

        public override IDisposable AcquireDistributedLock(string resource, TimeSpan timeout)
        {
            return MongoDistributedLock.Acquire($"HangFire:{resource}", timeout, Database, _options);
        }

        public override string CreateExpiredJob(Job job, IDictionary<string, string> parameters, DateTime createdAt, TimeSpan expireIn)
        {
            if (job == null)
                throw new ArgumentNullException(nameof(job));

            if (parameters == null)
                throw new ArgumentNullException(nameof(parameters));

            var invocationData = InvocationData.Serialize(job);

            var jobDto = new JobDto
            {
                InvocationData = JobHelper.ToJson(invocationData),
                Arguments = invocationData.Arguments,
                CreatedAt = createdAt,
                ExpireAt = createdAt.Add(expireIn)
            };

            Database.Job.InsertOne(jobDto);
            
            if (parameters.Count > 0)
            {
                Database.JobParameter.InsertMany(
                    parameters.Select(parameter => new JobParameterDto
                    {
                        JobId = jobDto.Id,
                        Name = parameter.Key,
                        Value = parameter.Value,
                        ExpireAt = jobDto.ExpireAt
                    }));
            }

            return jobDto.Id;
        }

        public override IFetchedJob FetchNextJob(string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null || queues.Length == 0)
                throw new ArgumentNullException(nameof(queues));

            var providers = queues
                .Select(queue => _queueProviders.GetProvider(queue))
                .Distinct()
                .ToArray();

            if (providers.Length != 1)
            {
                throw new InvalidOperationException(
                    $"Multiple provider instances registered for queues: {string.Join(", ", queues)}. You should choose only one type of persistent queues per server instance.");
            }

            var persistentQueue = providers[0].GetJobQueue();
            return persistentQueue.Dequeue(queues, cancellationToken);
        }

        public override void SetJobParameter(string jobId, string name, string value)
        {
            if (string.IsNullOrEmpty(jobId))
                throw new ArgumentNullException(nameof(jobId));

            if (string.IsNullOrEmpty(name))
                throw new ArgumentNullException(nameof(name));

            Database.JobParameter.UpdateOne(
                Q.FindJobParameterByJobIdAndName(jobId, name),
                Builders<JobParameterDto>.Update.Set(_ => _.Value, value),
                new UpdateOptions { IsUpsert = true });
        }

        public override string GetJobParameter(string jobId, string name)
        {
            if (string.IsNullOrEmpty(jobId))
                throw new ArgumentNullException(nameof(jobId));

            if (string.IsNullOrEmpty(name))
                throw new ArgumentNullException(nameof(name));

            return Database.JobParameter
                .Find(Q.FindJobParameterByJobIdAndName(jobId, name))
                .Project(Q.SelectJobParameterValue)
                .FirstOrDefault();
        }

        public override JobData GetJobData(string jobId)
        {
            if (string.IsNullOrEmpty(jobId))
                throw new ArgumentNullException(nameof(jobId));

            var jobData = Database.Job
                .Find(Q.FindJobById(jobId))
                .Project(Q.SelectJobAsPartial)
                .SingleOrDefault();

            if (jobData == null)
                return null;

            Job job = null;
            JobLoadException loadException = null;

            if (!string.IsNullOrEmpty(jobData.InvocationData))
            {
                // TODO: conversion exception could be thrown.
                var invocationData = JobHelper.FromJson<InvocationData>(jobData.InvocationData);
                invocationData.Arguments = jobData.Arguments;
                
                try
                {
                    job = invocationData.Deserialize();
                }
                catch (JobLoadException ex)
                {
                    loadException = ex;
                }
            }
            else
            {
                loadException = new JobLoadException("Empty InvocationData", null);
            }

            return new JobData
            {
                Job = job,
                State = jobData.StateName,
                CreatedAt = jobData.CreatedAt,
                LoadException = loadException
            };
        }

        public override StateData GetStateData(string jobId)
        {
            if (string.IsNullOrEmpty(jobId))
                throw new ArgumentNullException(nameof(jobId));

            return Database.Job
                .Find(Q.FindJobById(jobId))
                .Project(Q.SelectJobStateData)
                .SingleOrDefault();
        }

        public override void AnnounceServer(string serverId, ServerContext context)
        {
            if (string.IsNullOrEmpty(serverId))
                throw new ArgumentNullException(nameof(serverId));

            if (context == null)
                throw new ArgumentNullException(nameof(context));
            
            Database.Server.UpdateOne(
                Builders<ServerDto>.Filter.Eq(_ => _.Name, serverId),
                Builders<ServerDto>.Update.Set(_ => _.WorkerCount, context.WorkerCount)
                                          .Set(_ => _.Queues, context.Queues)
                                          .SetOnInsert(_ => _.StartedAt, Database.GetServerTimeUtc())
                                          .CurrentDate(_ => _.Heartbeat),
                new UpdateOptions { IsUpsert = true });
        }

        public override void RemoveServer(string serverId)
        {
            if (string.IsNullOrEmpty(serverId))
                throw new ArgumentNullException(nameof(serverId));

            Database.Server.DeleteOne(
                Builders<ServerDto>.Filter.Eq(_ => _.Name, serverId));
        }

        public override void Heartbeat(string serverId)
        {
            if (string.IsNullOrEmpty(serverId))
                throw new ArgumentNullException(nameof(serverId));

            Database.Server.UpdateOne(
                Builders<ServerDto>.Filter.Eq(_ => _.Name, serverId),
                Builders<ServerDto>.Update.CurrentDate(_ => _.Heartbeat));
        }

        public override int RemoveTimedOutServers(TimeSpan timeOut)
        {
            if (timeOut < TimeSpan.Zero)
                throw new ArgumentException("The `timeOut` value must be positive.", nameof(timeOut));

            return (int)Database.Server
                .DeleteMany(_ => _.Heartbeat < Database.GetServerTimeUtc() - timeOut)
                .DeletedCount;
        }

        public override HashSet<string> GetAllItemsFromSet(string key)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentNullException(nameof(key));

            return new HashSet<string>(Database.Set
                .Find(Q.FindSetByKey(key))
                .Project(Q.SelectSetValue)
                .ToList());
        }

        public override string GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentNullException(nameof(key));

            if (toScore < fromScore)
                throw new ArgumentException($"The `{nameof(toScore)}` value must be higher or equal to the `{nameof(fromScore)}` value.");
            
            return Database.Set
                .Find(Q.FindSetByKeyAndScore(key, fromScore, toScore))
                .Sort(Q.OrderSetByScore)
                .Project(Q.SelectSetValue)
                .FirstOrDefault();
        }

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentNullException(nameof(key));

            if (keyValuePairs == null)
                throw new ArgumentNullException(nameof(keyValuePairs));

            var updates = new List<WriteModel<HashDto>>();

            foreach (var keyValuePair in keyValuePairs)
            {
                updates.Add(new UpdateOneModel<HashDto>(
                    Q.FindHashByKeyAndField(key, keyValuePair.Key),
                    Builders<HashDto>.Update.Set(_ => _.Value, keyValuePair.Value))
                    { IsUpsert = true });
            }

            Database.Hash.BulkWrite(updates);
        }

        public override Dictionary<string, string> GetAllEntriesFromHash(string key)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentNullException(nameof(key));

            var result = Database.Hash
                .Find(Q.FindHashByKey(key))
                .Project(Q.SelectHashFieldAndValue)
                .ToDictionary(_ => _.Name, _ => _.Value);

            return result.Any() ? result : null;
        }

        public override long GetSetCount(string key)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentNullException(nameof(key));
            
            return Database.Set.Count(Q.FindSetByKey(key));
        }

        public override List<string> GetRangeFromSet(string key, int startingFrom, int endingAt)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentNullException(nameof(key));

            if (endingAt < startingFrom)
                throw new ArgumentException($"The `{nameof(endingAt)}` value must be higher or equal to the `{nameof(startingFrom)}` value.");

            return Database.Set
                .Find(Q.FindSetByKey(key))
                .Project(Q.SelectSetValue)
                .Skip(startingFrom)
                .Limit(endingAt - startingFrom + 1) // inclusive -- ensure the last element is included
                .ToList();
        }

        public override TimeSpan GetSetTtl(string key)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentNullException(nameof(key));

            var min = Database.Set
                .Find(Q.FindSetByKey(key) & Q.OnlyWithExpirationDate)
                .Sort(Q.OrderByExpirationDate)
                .Project(Q.SelectSetExpireAt)
                .FirstOrDefault();

            return min.HasValue ? (min.Value - Database.GetServerTimeUtc()) : NoTtl;
        }

        public override long GetCounter(string key)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentNullException(nameof(key));

            var counters = Database.Counter.Aggregate()
                .Match(Q.FindCountersByKey(key))
                .Group(Q.GroupCountersByKey)
                .SingleOrDefault();

            var aggregatedCount = Database.AggregatedCounter
                .Find(Q.FindAggregatedCounterByKey(key))
                .Project(Q.SelectAggregatedCounterValue)
                .SingleOrDefault();
            
            return (counters != null ? counters.Value : 0) + aggregatedCount;
        }

        public override long GetHashCount(string key)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentNullException(nameof(key));

            return Database.Hash.Count(Q.FindHashByKey(key));
        }

        public override TimeSpan GetHashTtl(string key)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentNullException(nameof(key));

            var min = Database.Hash
                .Find(Q.FindHashByKey(key) & Q.OnlyWithExpirationDate)
                .Sort(Q.OrderByExpirationDate)
                .Project(Q.SelectHashExpireAt)
                .FirstOrDefault();

            return min.HasValue ? (min.Value - Database.GetServerTimeUtc()) : NoTtl;
        }

        public override string GetValueFromHash(string key, string name)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentNullException(nameof(key));

            if (string.IsNullOrEmpty(name))
                throw new ArgumentNullException(nameof(name));

            return Database.Hash
                .Find(Q.FindHashByKeyAndField(key, name))
                .Project(Q.SelectHashValue)
                .FirstOrDefault();
        }

        public override long GetListCount(string key)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentNullException(nameof(key));

            return Database.List.Count(Q.FindListByKey(key));
        }

        public override TimeSpan GetListTtl(string key)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentNullException(nameof(key));

            var min = Database.List
                .Find(Q.FindListByKey(key) & Q.OnlyWithExpirationDate)
                .Sort(Q.OrderByExpirationDate)
                .Project(Q.SelectListExpireAt)
                .FirstOrDefault();

            return min.HasValue ? (min.Value - Database.GetServerTimeUtc()) : NoTtl;
        }

        public override List<string> GetRangeFromList(string key, int startingFrom, int endingAt)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentNullException(nameof(key));

            return Database.List
                .Find(Q.FindListByKey(key))
                .Project(Q.SelectListValue)
                .Skip(startingFrom)
                .Limit(endingAt - startingFrom + 1) // inclusive -- ensure the last element is included
                .ToList();
        }

        public override List<string> GetAllItemsFromList(string key)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentNullException(nameof(key));

            return Database.List
                .Find(Q.FindListByKey(key))
                .Project(Q.SelectListValue)
                .ToList();
        }
    }
}