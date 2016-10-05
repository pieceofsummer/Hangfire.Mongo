using System;
using System.Threading;
using Hangfire.Annotations;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.MongoUtils;
using Hangfire.Storage;
using MongoDB.Driver;

namespace Hangfire.Mongo.PersistentJobQueue.Mongo
{
    internal class MongoJobQueue : IPersistentJobQueue, IDisposable
    {
        private readonly ManualResetEvent _eventWaitHandle = new ManualResetEvent(false);

        private readonly MongoStorage _storage;
        private readonly MongoStorageOptions _options;
        private bool _disposed = false;
        
        public MongoJobQueue(MongoStorage storage, MongoStorageOptions options)
        {
            if (storage == null)
                throw new ArgumentNullException(nameof(storage));
            if (options == null)
                throw new ArgumentNullException(nameof(options));

            _storage = storage;
            _options = options;
        }

        [NotNull]
        public IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null) throw new ArgumentNullException(nameof(queues));
            if (queues.Length == 0) throw new ArgumentException("Queue array must be non-empty.", nameof(queues));
            
            if (_disposed)
                throw new ObjectDisposedException(nameof(MongoJobQueue));

            var triggers = new[]
            {
                cancellationToken.WaitHandle,   // task cancelation (receiver: current thread)
                _eventWaitHandle                // job added (receiver: all waiting threads)
            };

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var fetchedJob = _storage.Connection.JobQueue.FindOneAndUpdate(
                    Builders<JobQueueDto>.Filter.In(_ => _.Queue, queues) &
                    (Builders<JobQueueDto>.Filter.Eq(_ => _.FetchedAt, null) |
                     Builders<JobQueueDto>.Filter.Lt(_ => _.FetchedAt, _storage.Connection.GetServerTimeUtc() - _options.InvisibilityTimeout)),
                    Builders<JobQueueDto>.Update.CurrentDate(_ => _.FetchedAt),
                    new FindOneAndUpdateOptions<JobQueueDto> { ReturnDocument = ReturnDocument.After }, 
                    cancellationToken);

                if (fetchedJob != null)
                {
                    return new MongoFetchedJob(_storage.Connection, fetchedJob.Id, fetchedJob.JobId, fetchedJob.Queue);
                }

                var triggerId = WaitHandle.WaitAny(triggers, _options.QueuePollInterval);

                if (triggerId == 1 && _disposed)
                {
                    throw new ObjectDisposedException(nameof(MongoJobQueue));
                }
            }

        }

        public void Enqueue(string queue, string jobId)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(MongoJobQueue));

            _storage.Connection.JobQueue.InsertOne(new JobQueueDto
            {
                JobId = jobId,
                Queue = queue
            });
        }

        public void NotifyQueueChanged()
        {
            if (_disposed) return;

            // wake up all sleeping dequeuers, so they immediately start processing new jobs
            _eventWaitHandle.Set();
            _eventWaitHandle.Reset();
        }

        public void Dispose()
        {
            if (_disposed) return;
            
            _disposed = true;

            // wake up all sleeping dequeuers, so they immediately exit with ObjectDisposedException
            _eventWaitHandle.Set();
            _eventWaitHandle.Dispose();
        }
    }
}