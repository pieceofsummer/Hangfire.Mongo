using System;
using System.Linq;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.MongoUtils;
using Hangfire.Mongo.Tests.Utils;
using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;

namespace Hangfire.Mongo.Tests
{
#pragma warning disable 1591
    [Collection("Database")]
    public class MongoFetchedJobFacts
    {
        private const string Id = "57d468929d01c532184e77f6";
        private const string JobId = "579e47b79d01c5191c376260";
        private const string Queue = "queue";


        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => new MongoFetchedJob(null, Id, JobId, Queue));

                Assert.Equal("connection", exception.ParamName);
            });
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenIdIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => new MongoFetchedJob(connection, null, JobId, Queue));

                Assert.Equal("id", exception.ParamName);
            });
        }
        
        [Fact]
        public void Ctor_ThrowsAnException_WhenJobIdIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => new MongoFetchedJob(connection, Id, null, Queue));

                Assert.Equal("jobId", exception.ParamName);
            });
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenQueueIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => new MongoFetchedJob(connection, Id, JobId, null));

                Assert.Equal("queue", exception.ParamName);
            });
        }

        [Fact]
        public void Ctor_CorrectlySets_AllInstanceProperties()
        {
            UseConnection(connection =>
            {
                var fetchedJob = new MongoFetchedJob(connection, Id, JobId, Queue);

                Assert.Equal(Id, fetchedJob.Id);
                Assert.Equal(JobId, fetchedJob.JobId);
                Assert.Equal(Queue, fetchedJob.Queue);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveFromQueue_ReallyDeletesTheJobFromTheQueue()
        {
            UseConnection(connection =>
            {
                // Arrange
                var id = CreateJobQueueRecord(connection, JobId, "default");
                var processingJob = new MongoFetchedJob(connection, id, JobId, "default");

                // Act
                processingJob.RemoveFromQueue();

                // Assert
                var count = connection.JobQueue.Count(new BsonDocument());
                Assert.Equal(0, count);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveFromQueue_DoesNotDelete_UnrelatedJobs()
        {
            UseConnection(connection =>
            {
                // Arrange
                CreateJobQueueRecord(connection, JobId, "default");
                CreateJobQueueRecord(connection, NewId(), "critical");
                CreateJobQueueRecord(connection, NewId(), "default");

                var fetchedJob = new MongoFetchedJob(connection, NewId(), JobId, "default");

                // Act
                fetchedJob.RemoveFromQueue();

                // Assert
                var count = connection.JobQueue.Count(new BsonDocument());
                Assert.Equal(3, count);
            });
        }

        [Fact, CleanDatabase]
        public void Requeue_SetsFetchedAtValueToNull()
        {
            UseConnection(connection =>
            {
                // Arrange
                var id = CreateJobQueueRecord(connection, JobId, "default");
                var processingJob = new MongoFetchedJob(connection, id, JobId, "default");

                // Act
                processingJob.Requeue();

                // Assert
                var record = connection.JobQueue.Find(new BsonDocument()).ToList().Single();
                Assert.Null(record.FetchedAt);
            });
        }

        [Fact, CleanDatabase]
        public void Dispose_SetsFetchedAtValueToNull_IfThereWereNoCallsToComplete()
        {
            UseConnection(connection =>
            {
                // Arrange
                var id = CreateJobQueueRecord(connection, JobId, "default");
                var processingJob = new MongoFetchedJob(connection, id, JobId, "default");

                // Act
                processingJob.Dispose();

                // Assert
                var record = connection.JobQueue.Find(new BsonDocument()).ToList().Single();
                Assert.Null(record.FetchedAt);
            });
        }

        private static string NewId()
        {
            return ObjectId.GenerateNewId().ToString();
        }

        private static string CreateJobQueueRecord(HangfireDbContext connection, string jobId, string queue)
        {
            var jobQueue = new JobQueueDto
            {
                JobId = jobId,
                Queue = queue,
                FetchedAt = connection.GetServerTimeUtc()
            };

            connection.JobQueue.InsertOne(jobQueue);

            return jobQueue.Id;
        }

        private static void UseConnection(Action<HangfireDbContext> action)
        {
            using (var connection = ConnectionUtils.CreateConnection())
            {
                action(connection);
            }
        }
    }
#pragma warning restore 1591
}