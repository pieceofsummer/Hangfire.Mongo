using System;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Storage;
using MongoDB.Driver;

namespace Hangfire.Mongo
{
    /// <summary>
    /// Hangfire fetched job for Mongo database
    /// </summary>
    public sealed class MongoFetchedJob : IFetchedJob
    {
        private readonly HangfireDbContext _connection;
        private bool _disposed;
        private bool _updated;

        /// <summary>
        /// Constructs fetched job by database connection, identifier, job ID and queue
        /// </summary>
        /// <param name="connection">Database connection</param>
        /// <param name="jobId">Job ID</param>
        /// <param name="queue">Queue name</param>
        public MongoFetchedJob(HangfireDbContext connection, string jobId, string queue)
        {
            if (connection == null) throw new ArgumentNullException(nameof(connection));
            if (string.IsNullOrEmpty(jobId)) throw new ArgumentNullException(nameof(jobId));
            if (string.IsNullOrEmpty(queue)) throw new ArgumentNullException(nameof(queue));

            _connection = connection;
            
            JobId = jobId;
            Queue = queue;
        }
        
        /// <summary>
        /// Job ID
        /// </summary>
        public string JobId { get; private set; }

        /// <summary>
        /// Queue name
        /// </summary>
        public string Queue { get; private set; }
        
        /// <summary>
        /// Removes/requeues job as an atomic operation
        /// </summary>
        private void UpdateJob(bool requeue)
        {
            _connection.Job.UpdateOne(
                Builders<JobDto>.Filter.Eq(_ => _.Id, JobId),
                Builders<JobDto>.Update.Set(_ => _.FetchedAt, null)
                                       .Set(_ => _.Queue, requeue ? Queue : null));
        }

        /// <summary>
        /// Removes fetched job from a queue
        /// </summary>
        public void RemoveFromQueue()
        {
            UpdateJob(false);
            _updated = true;
        }

        /// <summary>
        /// Puts fetched job into a queue
        /// </summary>
        public void Requeue()
        {
            UpdateJob(true);
            _updated = true;
        }

        /// <summary>
        /// Disposes the object
        /// </summary>
        public void Dispose()
        {
            if (_disposed) return;

            if (!_updated)
            {
                // if the job was not explicitly updated 
                // (either removed or requeued), 
                // then requeue it by default
                Requeue();
            }

            _disposed = true;
        }
    }
}