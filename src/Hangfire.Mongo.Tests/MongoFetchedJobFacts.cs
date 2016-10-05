using System;
using System.Linq;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.Tests.Utils;
using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;

namespace Hangfire.Mongo.Tests
{
    [Collection("Database")]
    public class MongoFetchedJobFacts
    {
        private const string JobId = "579e47b79d01c5191c376260";
        private const string Queue = "queue";


        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionIsNull()
        {
            UseConnection(connection =>
            {
                Assert.Throws<ArgumentNullException>("connection",
                    () => new MongoFetchedJob(null, JobId, Queue));
            });
        }
        
        [Fact]
        public void Ctor_ThrowsAnException_WhenJobIdIsNull()
        {
            UseConnection(connection =>
            {
                Assert.Throws<ArgumentNullException>("jobId",
                    () => new MongoFetchedJob(connection, null, Queue));
            });
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenQueueIsNull()
        {
            UseConnection(connection =>
            {
                Assert.Throws<ArgumentNullException>("queue",
                    () => new MongoFetchedJob(connection, JobId, null));
            });
        }

        [Fact]
        public void Ctor_CorrectlySets_AllInstanceProperties()
        {
            UseConnection(connection =>
            {
                var fetchedJob = new MongoFetchedJob(connection, JobId, Queue);
                
                Assert.Equal(JobId, fetchedJob.JobId);
                Assert.Equal(Queue, fetchedJob.Queue);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveFromQueue_ClearsBothQueueAndFetchedAt()
        {
            UseConnection(connection =>
            {
                // Arrange
                var jobId = CreateFetchedJobRecord(connection, "default");
                var processingJob = new MongoFetchedJob(connection, jobId, "default");

                // Act
                processingJob.RemoveFromQueue();

                // Assert
                var job = connection.Job.AsQueryable().Single();
                Assert.Equal(jobId, job.Id);
                Assert.Equal(null, job.Queue);
                Assert.Equal(null, job.FetchedAt);
            });
        }
        
        [Fact, CleanDatabase]
        public void Requeue_PreservesQueueButClearsFetchedAt()
        {
            UseConnection(connection =>
            {
                // Arrange
                var jobId = CreateFetchedJobRecord(connection, "default");
                var processingJob = new MongoFetchedJob(connection, jobId, "default");

                // Act
                processingJob.Requeue();

                // Assert
                var record = connection.Job.AsQueryable().Single();
                Assert.Equal(jobId, record.Id);
                Assert.Equal("default", record.Queue);
                Assert.Null(record.FetchedAt);
            });
        }

        [Fact, CleanDatabase]
        public void Dispose_RequeuesJobByDefault_IfNoActionWasPerformed()
        {
            UseConnection(connection =>
            {
                // Arrange
                var jobId = CreateFetchedJobRecord(connection, "default");
                var processingJob = new MongoFetchedJob(connection, jobId, "default");

                // Act
                processingJob.Dispose();

                // Assert
                var record = connection.Job.AsQueryable().Single();
                Assert.Equal(jobId, record.Id);
                Assert.Equal("default", record.Queue);
                Assert.Null(record.FetchedAt);
            });
        }
        
        private static string CreateFetchedJobRecord(HangfireDbContext connection, string queue)
        {
            var job = new JobDto
            {
                InvocationData = "",
                Arguments = "",
                Queue = queue,
                FetchedAt = DateTime.UtcNow
            };

            connection.Job.InsertOne(job);

            return job.Id;
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