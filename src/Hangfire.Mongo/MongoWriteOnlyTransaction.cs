using System;
using System.Collections.Generic;
using System.Linq;
using Hangfire.Common;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.PersistentJobQueue;
using Hangfire.States;
using Hangfire.Storage;
using MongoDB.Driver;

namespace Hangfire.Mongo
{
    internal sealed class MongoWriteOnlyTransaction : JobStorageTransaction
    {
        private readonly Queue<Action<HangfireDbContext>> _commandQueue 
            = new Queue<Action<HangfireDbContext>>();

        private readonly HashSet<IPersistentJobQueue> _changedQueues
            = new HashSet<IPersistentJobQueue>();

        private readonly HangfireDbContext _connection;

        private readonly PersistentJobQueueProviderCollection _queueProviders;

        private DateTime? _serverTime = null;

        public MongoWriteOnlyTransaction(HangfireDbContext connection, PersistentJobQueueProviderCollection queueProviders)
        {
            if (connection == null)
                throw new ArgumentNullException(nameof(connection));

            if (queueProviders == null)
                throw new ArgumentNullException(nameof(queueProviders));

            _connection = connection;
            _queueProviders = queueProviders;
        }

        public override void Dispose()
        {
        }
        
        private DateTime ServerTime
        {
            get
            {
                if (_serverTime == null)
                {
                    // query server time once per transaction
                    _serverTime = _connection.GetServerTimeUtc();
                }

                return _serverTime.Value;
            }
        }

        public override void ExpireJob(string jobId, TimeSpan expireIn)
        {
            if (string.IsNullOrEmpty(jobId)) throw new ArgumentNullException(nameof(jobId));
            
            QueueCommand(x =>
            {
                var expireAt = ServerTime + expireIn;

                x.Job.UpdateOne(
                    Builders<JobDto>.Filter.Eq(_ => _.Id, jobId),
                    Builders<JobDto>.Update.Set(_ => _.ExpireAt, expireAt));

                // set expiration on associated JobParameters
                x.JobParameter.UpdateMany(
                    Builders<JobParameterDto>.Filter.Eq(_ => _.JobId, jobId),
                    Builders<JobParameterDto>.Update.Set(_ => _.ExpireAt, expireAt));

                // set expiration on asspciated States
                x.State.UpdateMany(
                    Builders<StateDto>.Filter.Eq(_ => _.JobId, jobId),
                    Builders<StateDto>.Update.Set(_ => _.ExpireAt, expireAt));
            });
        }

        public override void PersistJob(string jobId)
        {
            if (string.IsNullOrEmpty(jobId)) throw new ArgumentNullException(nameof(jobId));

            QueueCommand(x =>
            {
                x.Job.UpdateOne(
                    Builders<JobDto>.Filter.Eq(_ => _.Id, jobId),
                    Builders<JobDto>.Update.Set(_ => _.ExpireAt, null));

                // clear expiration on associated JobParameters
                x.JobParameter.UpdateMany(
                    Builders<JobParameterDto>.Filter.Eq(_ => _.JobId, jobId),
                    Builders<JobParameterDto>.Update.Set(_ => _.ExpireAt, null));

                // clear expiration on asspciated States
                x.State.UpdateMany(
                    Builders<StateDto>.Filter.Eq(_ => _.JobId, jobId),
                    Builders<StateDto>.Update.Set(_ => _.ExpireAt, null));
            });
        }
        
        public override void SetJobState(string jobId, IState state)
        {
            if (string.IsNullOrEmpty(jobId)) throw new ArgumentNullException(nameof(jobId));
            if (state == null) throw new ArgumentNullException(nameof(state));

            QueueCommand(x =>
            {
                var stateDto = new StateDto
                {
                    JobId = jobId,
                    Name = state.Name,
                    Reason = state.Reason,
                    CreatedAt = ServerTime,
                    Data = JobHelper.ToJson(state.SerializeData())
                };

                x.State.InsertOne(stateDto);

                x.Job.UpdateOne(
                    Builders<JobDto>.Filter.Eq(_ => _.Id, jobId),
                    Builders<JobDto>.Update.Set(_ => _.StateId, stateDto.Id)
                                           .Set(_ => _.StateName, state.Name));
            });
        }

        public override void AddJobState(string jobId, IState state)
        {
            if (string.IsNullOrEmpty(jobId)) throw new ArgumentNullException(nameof(jobId));
            if (state == null) throw new ArgumentNullException(nameof(state));

            QueueCommand(x => x.State.InsertOne(new StateDto
            {
                JobId = jobId,
                Name = state.Name,
                Reason = state.Reason,
                CreatedAt = ServerTime,
                Data = JobHelper.ToJson(state.SerializeData())
            }));
        }

        public override void AddToQueue(string queue, string jobId)
        {
            if (string.IsNullOrEmpty(queue)) throw new ArgumentNullException(nameof(queue));
            if (string.IsNullOrEmpty(jobId)) throw new ArgumentNullException(nameof(jobId));

            IPersistentJobQueueProvider provider = _queueProviders.GetProvider(queue);
            IPersistentJobQueue persistentQueue = provider.GetJobQueue();

            QueueCommand(_ =>
            {
                persistentQueue.Enqueue(queue, jobId);
            });

            _changedQueues.Add(persistentQueue);
        }

        public override void IncrementCounter(string key)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            QueueCommand(x => x.Counter.InsertOne(new CounterDto
            {
                Key = key,
                Value = +1
            }));
        }

        public override void IncrementCounter(string key, TimeSpan expireIn)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            QueueCommand(x => x.Counter.InsertOne(new CounterDto
            {
                Key = key,
                Value = +1,
                ExpireAt = ServerTime + expireIn
            }));
        }

        public override void DecrementCounter(string key)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            QueueCommand(x => x.Counter.InsertOne(new CounterDto
            {
                Key = key,
                Value = -1
            }));
        }

        public override void DecrementCounter(string key, TimeSpan expireIn)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            QueueCommand(x => x.Counter.InsertOne(new CounterDto
            {
                Key = key,
                Value = -1,
                ExpireAt = ServerTime + expireIn
            }));
        }

        public override void AddToSet(string key, string value)
        {
            AddToSet(key, value, 0.0);
        }

        public override void AddToSet(string key, string value, double score)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            QueueCommand(x => x.Set.UpdateOne(
                Builders<SetDto>.Filter.Eq(_ => _.Key, key) &
                Builders<SetDto>.Filter.Eq(_ => _.Value, value),
                Builders<SetDto>.Update.Set(_ => _.Score, score),
                new UpdateOptions { IsUpsert = true }));
        }

        public override void RemoveFromSet(string key, string value)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            QueueCommand(x => x.Set.DeleteOne(
                Builders<SetDto>.Filter.Eq(_ => _.Key, key) &
                Builders<SetDto>.Filter.Eq(_ => _.Value, value)));
        }

        public override void InsertToList(string key, string value)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            QueueCommand(x => x.List.InsertOne(new ListDto
            {
                Key = key,
                Value = value
            }));
        }

        public override void RemoveFromList(string key, string value)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            QueueCommand(x => x.List.DeleteMany(
                Builders<ListDto>.Filter.Eq(_ => _.Key, key) &
                Builders<ListDto>.Filter.Eq(_ => _.Value, value)));
        }

        public override void TrimList(string key, int keepStartingFrom, int keepEndingAt)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            QueueCommand(x =>
            {
                var ids = x.List.AsQueryable()
                    .Where(_ => _.Key == key)
                    .OrderByDescending(_ => _.Id)
                    .Select(_ => _.Id)
                    .ToList() // materialize
                    .Where((id, index) => index < keepStartingFrom || index > keepEndingAt);
                
                x.List.DeleteMany(
                    Builders<ListDto>.Filter.Eq(_ => _.Key, key) &
                    Builders<ListDto>.Filter.In(_ => _.Id, ids));
            });
        }

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (keyValuePairs == null) throw new ArgumentNullException(nameof(keyValuePairs));

            QueueCommand(x => x.Hash.BulkWrite(
                keyValuePairs.Select(pair => new UpdateOneModel<HashDto>(
                    Builders<HashDto>.Filter.Eq(_ => _.Key, key) &
                    Builders<HashDto>.Filter.Eq(_ => _.Field, pair.Key),
                    Builders<HashDto>.Update.Set(_ => _.Value, pair.Value))
                    { IsUpsert = true })));
        }

        public override void RemoveHash(string key)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            QueueCommand(x => x.Hash.DeleteMany(
                Builders<HashDto>.Filter.Eq(_ => _.Key, key)));
        }

        public override void Commit()
        {
	        foreach (var action in _commandQueue)
	        {
		        action.Invoke(_connection);
	        }

            foreach (var queue in _changedQueues)
            {
                queue.NotifyQueueChanged();
            }
        }

        private void QueueCommand(Action<HangfireDbContext> action)
        {
            _commandQueue.Enqueue(action);
        }



        //New methods to support Hangfire pro feature - batches.




        public override void ExpireSet(string key, TimeSpan expireIn)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            QueueCommand(x => x.Set.UpdateMany(
                Builders<SetDto>.Filter.Eq(_ => _.Key, key),
                Builders<SetDto>.Update.Set(_ => _.ExpireAt, ServerTime + expireIn)));
        }

        public override void ExpireList(string key, TimeSpan expireIn)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            QueueCommand(x => x.List.UpdateMany(
                Builders<ListDto>.Filter.Eq(_ => _.Key, key),
                Builders<ListDto>.Update.Set(_ => _.ExpireAt, ServerTime + expireIn)));
        }

        public override void ExpireHash(string key, TimeSpan expireIn)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            QueueCommand(x => x.Hash.UpdateMany(
                Builders<HashDto>.Filter.Eq(_ => _.Key, key),
                Builders<HashDto>.Update.Set(_ => _.ExpireAt, ServerTime + expireIn)));
        }

        public override void PersistSet(string key)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            QueueCommand(x => x.Set.UpdateMany(
                Builders<SetDto>.Filter.Eq(_ => _.Key, key),
                Builders<SetDto>.Update.Set(_ => _.ExpireAt, null)));
        }

        public override void PersistList(string key)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            QueueCommand(x => x.List.UpdateMany(
                Builders<ListDto>.Filter.Eq(_ => _.Key, key),
                Builders<ListDto>.Update.Set(_ => _.ExpireAt, null)));
        }

        public override void PersistHash(string key)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            QueueCommand(x => x.Hash.UpdateMany(
                Builders<HashDto>.Filter.Eq(_ => _.Key, key),
                Builders<HashDto>.Update.Set(_ => _.ExpireAt, null)));
        }

        public override void AddRangeToSet(string key, IList<string> items)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (items == null) throw new ArgumentNullException(nameof(items));

            QueueCommand(x => x.Set.BulkWrite(
                items.Select(item => new UpdateOneModel<SetDto>(
                    Builders<SetDto>.Filter.Eq(_ => _.Key, key) &
                    Builders<SetDto>.Filter.Eq(_ => _.Value, item),
                    Builders<SetDto>.Update.Set(_ => _.Score, 0.0))
                    { IsUpsert = true })));
        }

        public override void RemoveSet(string key)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            QueueCommand(x => x.Set.DeleteMany(
                Builders<SetDto>.Filter.Eq(_ => _.Key, key)));
        }
    }
}