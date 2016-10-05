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
                Assert.Throws<ArgumentNullException>("connection",
                    () => new MongoFetchedJob(null, Id, JobId, Queue));
            });
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenIdIsNull()
        {
            UseConnection(connection =>
            {
                Assert.Throws<ArgumentNullException>("id",
                    () => new MongoFetchedJob(connection, null, JobId, Queue));
            });
        }
        
        [Fact]
        public void Ctor_ThrowsAnException_WhenJobIdIsNull()
        {
            UseConnection(connection =>
            {
                Assert.Throws<ArgumentNullException>("jobId",
                    () => new MongoFetchedJob(connection, Id, null, Queue));
            });
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenQueueIsNull()
        {
            UseConnection(connection =>
            {
                Assert.Throws<ArgumentNullException>("queue",
                    () => new MongoFetchedJob(connection, Id, JobId, null));
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
                var count = connection.JobQueue.AsQueryable().Count();
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
                CreateJobQueueRecord(connection, null, "critical");
                CreateJobQueueRecord(connection, null, "default");

                var fetchedJob = new MongoFetchedJob(connection, Id, JobId, "default");

                // Act
                fetchedJob.RemoveFromQueue();

                // Assert
                var count = connection.JobQueue.AsQueryable().Count();
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
                var record = connection.JobQueue.AsQueryable().Single();
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
                var record = connection.JobQueue.AsQueryable().Single();
                Assert.Null(record.FetchedAt);
            });
        }
        
        private static string CreateJobQueueRecord(HangfireDbContext connection, string jobId, string queue)
        {
            if (jobId == null)
                jobId = ObjectId.GenerateNewId().ToString();

            var jobQueue = new JobQueueDto
            {
                JobId = jobId,
                Queue = queue,
                FetchedAt = DateTime.UtcNow
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
}