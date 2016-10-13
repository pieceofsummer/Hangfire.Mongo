using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.States;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using System;
using System.Collections.Generic;

namespace Hangfire.Mongo
{
    internal static class Q
    {
        public const string SucceededCounterKey = "stats:succeeded";
        public const string DeletedCounterKey = "stats:deleted";
        public const string RecurringJobsSetKey = "recurring-jobs";

        [BsonIgnoreExtraElements]
        public sealed class KeyValueDto
        {
            [BsonId]
            public string Key { get; set; }

            [BsonRequired]
            public long Value { get; set; }
        }

        [BsonNoId, BsonIgnoreExtraElements]
        public sealed class EnqueuedAndTotalDto
        {
            [BsonRequired]
            public int Enqueued { get; set; }

            [BsonRequired]
            public int Total { get; set; }
        }

        [BsonIgnoreExtraElements]
        public sealed class PartialJobDto
        {
            [BsonId, BsonRepresentation(BsonType.ObjectId)]
            public string Id { get; set; }

            [BsonRequired]
            public DateTime CreatedAt { get; set; }

            [BsonRequired]
            public string StateName { get; set; }

            [BsonIgnoreIfNull]
            public string InvocationData { get; set; }

            [BsonIgnoreIfNull]
            public string Arguments { get; set; }
        }

        [BsonIgnoreExtraElements]
        public sealed class NameValueDto
        {
            [BsonRequired]
            public string Name { get; set; }

            public string Value { get; set; }
        }

        public static readonly BsonDocument OnlyWithExpirationDate =
            new BsonDocument("ExpireAt", new BsonDocument("$ne", BsonNull.Value));

        public static readonly BsonDocument OrderByExpirationDate =
            new BsonDocument("ExpireAt", 1);


        #region Job

        public static FilterDefinition<JobDto> FindJobsInState(string state)
        {
            return Builders<JobDto>.Filter.Eq("StateName", state);
        }

        public static FilterDefinition<JobDto> FindJobsInQueue(string queue)
        {
            return Builders<JobDto>.Filter.Eq("Queue", queue);
        }

        public static FilterDefinition<JobDto> FindJobsByIds(IEnumerable<string> jobIds)
        {
            return Builders<JobDto>.Filter.In(_ => _.Id, jobIds);
        }

        public static FilterDefinition<JobDto> FindJobById(string jobId)
        {
            return Builders<JobDto>.Filter.Eq(_ => _.Id, jobId);
        }

        public static readonly ProjectionDefinition<JobDto, EnqueuedAndTotalDto> CountJobsEnqueuedAndFetched =
            new JsonProjectionDefinition<JobDto, EnqueuedAndTotalDto>("{ _id: 0, Enqueued: { $sum: { $ifNull: [ '$FetchedAt', 1 ] } }, Total: { $sum: 1 } }").Cached();
        
        public static readonly FilterDefinition<JobDto> AllJobsInScheduledEnqueuedFailedProcessingStates
            = Builders<JobDto>.Filter.In(_ => _.StateName, new[] {
                ScheduledState.StateName, EnqueuedState.StateName, FailedState.StateName, ProcessingState.StateName
            }).Cached();

        public static readonly ProjectionDefinition<JobDto, KeyValueDto> GroupJobsByStateName =
            new JsonProjectionDefinition<JobDto, KeyValueDto>("{ _id: '$StateName', Value: { $sum: 1 } }").Cached();
        
        public static readonly FilterDefinition<JobDto> AllJobsWithQueueName = 
            (Builders<JobDto>.Filter.Exists(_ => _.Queue, true) & Builders<JobDto>.Filter.Ne(_ => _.Queue, null)).Cached();

        public static readonly FilterDefinition<JobDto> OnlyEnqueuedJobs = 
            Builders<JobDto>.Filter.Eq(_ => _.FetchedAt, null).Cached();

        public static readonly FilterDefinition<JobDto> OnlyFetchedJobs = 
            Builders<JobDto>.Filter.Ne(_ => _.FetchedAt, null).Cached();

        public static readonly FieldDefinition<JobDto, string> DistinctJobQueue =
            new ExpressionFieldDefinition<JobDto, string>(_ => _.Queue).Cached();
        
        public static readonly ProjectionDefinition<JobDto, PartialJobDto> SelectJobAsPartial
            = Builders<JobDto>.Projection.Expression(_ => new PartialJobDto {
                Id = _.Id, CreatedAt = _.CreatedAt, StateName = _.StateName, InvocationData = _.InvocationData, Arguments = _.Arguments
            }).Cached();
        
        public static readonly ProjectionDefinition<JobDto, string> SelectJobId = 
            Builders<JobDto>.Projection.Expression(_ => _.Id).Cached();
        
        public static readonly ProjectionDefinition<JobDto, StateData> SelectJobStateData = 
            Builders<JobDto>.Projection.Expression(_ => new StateData {
                Name = _.StateName, Reason = _.StateReason, Data = _.StateData
            }).Cached();
        
        public static readonly SortDefinition<JobDto> OrderJobById = 
            Builders<JobDto>.Sort.Ascending(_ => _.Id).Cached();

        public static readonly SortDefinition<JobDto> OrderJobByIdDescending = 
            Builders<JobDto>.Sort.Descending(_ => _.Id).Cached();

        #endregion

        #region JobParameter
        
        public static FilterDefinition<JobParameterDto> FindJobParametersByJobId(string jobId)
        {
            return Builders<JobParameterDto>.Filter.Eq("JobId", jobId);
        }

        public static FilterDefinition<JobParameterDto> FindJobParameterByJobIdAndName(string jobId, string name)
        {
            return Builders<JobParameterDto>.Filter.Eq("JobId", jobId)
                 & Builders<JobParameterDto>.Filter.Eq("Name", name);
        }
        
        public static readonly ProjectionDefinition<JobParameterDto, string> SelectJobParameterValue = 
            Builders<JobParameterDto>.Projection.Expression(_ => _.Value).Cached();

        public static readonly ProjectionDefinition<JobParameterDto, NameValueDto> SelectJobParameterNameAndValue = 
            Builders<JobParameterDto>.Projection.Expression(_ => new NameValueDto { Name = _.Name, Value = _.Value }).Cached();

        #endregion

        #region State

        public static FilterDefinition<StateDto> FindStatesByJobId(string jobId)
        {
            return Builders<StateDto>.Filter.Eq("JobId", jobId);
        }
        
        public static readonly ProjectionDefinition<StateDto, StateHistoryDto> SelectStateForHistory =
            Builders<StateDto>.Projection.Expression(_ => new StateHistoryDto {
                StateName = _.Name, CreatedAt = _.CreatedAt, Reason = _.Reason, Data = _.Data
            }).Cached();
        
        public static readonly SortDefinition<StateDto> OrderStateByCreatedAtDescending = 
            Builders<StateDto>.Sort.Descending(_ => _.CreatedAt).Cached();
        
        #endregion

        #region Set

        public static FilterDefinition<SetDto> FindSetByKey(string key)
        {
            return Builders<SetDto>.Filter.Eq("Key", key);
        }

        public static FilterDefinition<SetDto> FindSetByKeyAndScore(string key, double fromScore, double toScore)
        {
            return Builders<SetDto>.Filter.Eq("Key", key)
                 & Builders<SetDto>.Filter.Gte("Score", fromScore)
                 & Builders<SetDto>.Filter.Lte("Score", toScore);
        }
        
        public static readonly FilterDefinition<SetDto> FindRecurringJobsSet =
            Builders<SetDto>.Filter.Eq(_ => _.Key, RecurringJobsSetKey).Cached();

        public static readonly ProjectionDefinition<SetDto, string> SelectSetValue = 
            Builders<SetDto>.Projection.Expression(_ => _.Value).Cached();

        public static readonly ProjectionDefinition<SetDto, DateTime?> SelectSetExpireAt = 
            Builders<SetDto>.Projection.Expression(_ => _.ExpireAt).Cached();

        public static readonly SortDefinition<SetDto> OrderSetByScore = 
            Builders<SetDto>.Sort.Ascending(_ => _.Score).Cached();
        
        #endregion

        #region Hash

        public static FilterDefinition<HashDto> FindHashByKey(string key)
        {
            return Builders<HashDto>.Filter.Eq("Key", key);
        }

        public static FilterDefinition<HashDto> FindHashByKeyAndField(string key, string field)
        {
            return Builders<HashDto>.Filter.Eq("Key", key)
                 & Builders<HashDto>.Filter.Eq("Field", field);
        }
        
        public static readonly ProjectionDefinition<HashDto, string> SelectHashValue = 
            Builders<HashDto>.Projection.Expression(_ => _.Value).Cached();

        public static readonly ProjectionDefinition<HashDto, NameValueDto> SelectHashFieldAndValue = 
            Builders<HashDto>.Projection.Expression(_ => new NameValueDto { Name = _.Field, Value = _.Value }).Cached();

        public static readonly ProjectionDefinition<HashDto, DateTime?> SelectHashExpireAt =
            Builders<HashDto>.Projection.Expression(_ => _.ExpireAt).Cached();

        #endregion

        #region List

        public static FilterDefinition<ListDto> FindListByKey(string key)
        {
            return Builders<ListDto>.Filter.Eq("Key", key);
        }
        
        public static readonly ProjectionDefinition<ListDto, string> SelectListValue = 
            Builders<ListDto>.Projection.Expression(_ => _.Value).Cached();

        public static readonly ProjectionDefinition<ListDto, DateTime?> SelectListExpireAt = 
            Builders<ListDto>.Projection.Expression(_ => _.ExpireAt).Cached();

        #endregion

        #region Counters and AggregatedCounters
        
        public static FilterDefinition<CounterDto> FindCountersByKey(string key)
        {
            return Builders<CounterDto>.Filter.Eq("Key", key);
        }

        public static readonly ProjectionDefinition<CounterDto, KeyValueDto> GroupCountersByKey =
            new JsonProjectionDefinition<CounterDto, KeyValueDto>("{ _id: '$Key', Value: { $sum: '$Value' } }").Cached();

        public static readonly FilterDefinition<CounterDto> SucceededAndDeletedCounters =
            Builders<CounterDto>.Filter.In(_ => _.Key, new[] { SucceededCounterKey, DeletedCounterKey }).Cached();

        public static FilterDefinition<AggregatedCounterDto> FindAggregatedCounterByKey(string key)
        {
            return Builders<AggregatedCounterDto>.Filter.Eq("_id", key);
        }

        public static FilterDefinition<AggregatedCounterDto> FindAggregatedCountersByKeys(IEnumerable<string> keys)
        {
            return Builders<AggregatedCounterDto>.Filter.In("_id", keys);
        }
        
        public static readonly ProjectionDefinition<AggregatedCounterDto, long> SelectAggregatedCounterValue = 
            Builders<AggregatedCounterDto>.Projection.Expression(_ => _.Value).Cached();
        
        public static readonly FilterDefinition<AggregatedCounterDto> SucceededAndDeletedAggregatedCounters =
            Builders<AggregatedCounterDto>.Filter.In(_ => _.Key, new[] { SucceededCounterKey, DeletedCounterKey }).Cached();
        
        #endregion

    }
}
