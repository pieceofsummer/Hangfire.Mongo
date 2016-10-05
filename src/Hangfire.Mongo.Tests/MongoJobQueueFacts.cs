using System;
using System.Linq;
using System.Threading;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.PersistentJobQueue.Mongo;
using Hangfire.Mongo.Tests.Utils;
using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;
using Hangfire.Mongo.Database;
using System.Threading.Tasks;
using System.Diagnostics;
using Hangfire.Storage;
using System.Runtime.ExceptionServices;
using Hangfire.Server;

namespace Hangfire.Mongo.Tests
{
    [Collection("Database")]
    public class MongoJobQueueFacts
    {
        private static readonly string[] DefaultQueues = { "default" };

        [Fact]
        public void Ctor_ThrowsAnException_WhenStorageIsNull()
        {
            Assert.Throws<ArgumentNullException>("storage",
                () => new MongoJobQueue(null, new MongoStorageOptions()));
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenOptionsIsNull()
        {
            UseConnection(storage =>
            {
                Assert.Throws<ArgumentNullException>("options",
                   () => new MongoJobQueue(storage, null));
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldThrowAnException_WhenQueuesCollectionIsNull()
        {
            UseConnection((connection, queue) =>
            {
                Assert.Throws<ArgumentNullException>("queues",
                    () => queue.Dequeue(null, CreateTimingOutCancellationToken()));
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldThrowAnException_WhenQueuesCollectionIsEmpty()
        {
            UseConnection((connection, queue) =>
            {
                Assert.Throws<ArgumentException>("queues",
                    () => queue.Dequeue(new string[0], CreateTimingOutCancellationToken()));
            });
        }

        [Fact]
        public void Dequeue_ThrowsOperationCanceled_WhenCancellationTokenIsSetAtTheBeginning()
        {
            UseConnection((connection, queue) =>
            {
                var cts = new CancellationTokenSource();
                cts.Cancel();

                Assert.Throws<OperationCanceledException>(
                    () => queue.Dequeue(DefaultQueues, cts.Token));
            });
        }

        [Fact]
        public void Dequeue_ThrowsObjectDisposed_IfDisposedAtTheBeginning()
        {
            UseConnection((connection, queue) =>
            {
                var cts = new CancellationTokenSource();
                cts.Cancel();

                queue.Dispose();

                Assert.Throws<ObjectDisposedException>(
                    () => queue.Dequeue(DefaultQueues, cts.Token));
            });
        }
        
        [Fact, CleanDatabase]
        public void Dequeue_ThrowsObjectDisposed_OnAllWaitingThreads_WhenDisposed()
        {
            UseConnection((connection, queue) =>
            {
                var cts = new CancellationTokenSource(1000);

                Action defaultQueueHandler = 
                    () => Assert.Throws<ObjectDisposedException>(
                        () => queue.Dequeue(DefaultQueues, cts.Token));
                
                using (new ParallelThread(defaultQueueHandler))
                using (new ParallelThread(defaultQueueHandler))
                using (new ParallelThread(defaultQueueHandler))
                {
                    Thread.Sleep(500); // give threads some time to enter 'waiting' state
                    queue.Dispose();
                }
            });
        }
        
        [Fact, CleanDatabase]
        public void Dequeue_ShouldWaitIndefinitely_WhenThereAreNoJobs()
        {
            UseConnection((connection, queue) =>
            {
                var cts = new CancellationTokenSource(200);

                Assert.Throws<OperationCanceledException>(
                    () => queue.Dequeue(DefaultQueues, cts.Token));
            });
        }
        
        [Fact, CleanDatabase]
        public void Dequeue_ShouldContinueWaiting_WhenNotifiedButNoActualJobFetched()
        {
            UseConnection((connection, queue) =>
            {
                var cts = new CancellationTokenSource(1000);

                using (new ParallelThread(
                    () => Assert.Throws<OperationCanceledException>(
                        () => queue.Dequeue(DefaultQueues, cts.Token))))
                {
                    Thread.Sleep(500); // give thread some time to enter 'waiting' state
                    queue.NotifyQueueChanged();
                }
            });
        }
        
        [Fact, CleanDatabase]
        public void Dequeue_ShouldFetchAJob_FromTheSpecifiedQueue()
        {
            // Arrange
            UseConnection((connection, queue) =>
            {
                var jobId = CreateJobRecord(connection, "default");
                
                // Act
                var fetchedJob = (MongoFetchedJob)queue.Dequeue(DefaultQueues, CreateTimingOutCancellationToken());

                // Assert
                Assert.Equal(jobId, fetchedJob.JobId);
                Assert.Equal("default", fetchedJob.Queue);
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_PreservesQueueButSetsFetchedAt()
        {
            // Arrange
            UseConnection((connection, queue) =>
            {
                var jobId = CreateJobRecord(connection, "default");
                
                // Act
                var fetchedJob = queue.Dequeue(DefaultQueues, CreateTimingOutCancellationToken());

                // Assert
                Assert.NotNull(fetchedJob);

                var record = connection.Job.AsQueryable().Single(_ => _.Id == fetchedJob.JobId);

                Assert.Equal("default", record.Queue);
                Assert.NotNull(record.FetchedAt);
                Assert.True(record.FetchedAt > DateTime.UtcNow.AddMinutes(-1));
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldFetchTimedOutJobs_FromTheSpecifiedQueue()
        {
            // Arrange
            UseConnection((connection, queue) =>
            {
                var job = new JobDto
                {
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = DateTime.UtcNow,
                    Queue = "default",
                    FetchedAt = DateTime.UtcNow.AddDays(-1)
                };
                connection.Job.InsertOne(job);
                
                // Act
                var fetchedJob = queue.Dequeue(DefaultQueues, CreateTimingOutCancellationToken());

                // Assert
                Assert.Equal(job.Id, fetchedJob.JobId);
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldSetFetchedAt_OnlyForTheFetchedJob()
        {
            // Arrange
            UseConnection((connection, queue) =>
            {
                var jobId1 = CreateJobRecord(connection, "critical");

                var jobId2 = CreateJobRecord(connection, "default");
                
                // Act
                var fetchedJob = queue.Dequeue(DefaultQueues, CreateTimingOutCancellationToken());

                // Assert
                var unfetchedJob = connection.Job.AsQueryable().Single(_ => _.Id != fetchedJob.JobId);

                Assert.Null(unfetchedJob.FetchedAt);
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldFetchJobs_OnlyFromSpecifiedQueues()
        {
            UseConnection((connection, queue) =>
            {
                var jobId = CreateJobRecord(connection, "critical");
                
                Assert.Throws<OperationCanceledException>(
                    () => queue.Dequeue(DefaultQueues, CreateTimingOutCancellationToken()));
            });
        }
        
        [Fact, CleanDatabase]
        public void Dequeue_ShouldFetchJobs_OnAllWaitingThreads_WhenNotified()
        {
            UseConnection((connection, queue) =>
            {
                var defaultJobId = CreateJobRecord(connection, null);
                var customJobId = CreateJobRecord(connection, null);

                var cts = new CancellationTokenSource(1000);
                IFetchedJob defaultJob = null, customJob = null;

                // Ensures the job is dequeued from default queue just once, 
                // while all the following attempts will eventually timeout
                Action defaultQueueHandler =
                    () => Assert.Throws<OperationCanceledException>(
                        (Action)delegate
                        {
                            var job = queue.Dequeue(DefaultQueues, cts.Token);
                            if (job == null)
                                throw new InvalidOperationException("dequeued a null job");
                            
                            if (Interlocked.CompareExchange(ref defaultJob, job, null) != null)
                                throw new InvalidOperationException("defaultJob was already assigned");

                            throw new OperationCanceledException("fake OperationCanceledException to pass test");
                        });


                using (new ParallelThread(() => customJob = queue.Dequeue(new[] { "custom" }, cts.Token)))
                using (new ParallelThread(defaultQueueHandler))
                using (new ParallelThread(defaultQueueHandler))
                using (new ParallelThread(defaultQueueHandler))
                {
                    Thread.Sleep(500); // give threads some time to enter 'waiting' state
                    queue.Enqueue("default", defaultJobId);
                    queue.Enqueue("custom", customJobId);
                    queue.NotifyQueueChanged();
                }

                Assert.Equal(defaultJobId, defaultJob.JobId);

                Assert.NotNull(defaultJob);
                Assert.Equal(defaultJobId, defaultJob.JobId);

                Assert.NotNull(customJob);
                Assert.Equal(customJobId, customJob.JobId);
            });
        }

        [Fact, CleanDatabase]
        public void Enqueue_AddsAJobToTheQueue()
        {
            UseConnection((connection, queue) =>
            {
                var jobId = CreateJobRecord(connection, null);
                
                queue.Enqueue("default", jobId);

                var fetchedJob = connection.Job.AsQueryable().Single();

                Assert.Equal(jobId, fetchedJob.Id);
                Assert.Equal("default", fetchedJob.Queue);
                Assert.Null(fetchedJob.FetchedAt);
            });
        }
        
        private static string CreateJobRecord(HangfireDbContext connection, string queue)
        {
            var job = new JobDto
            {
                InvocationData = "",
                Arguments = "",
                Queue = queue
            };

            connection.Job.InsertOne(job);
            return job.Id;
        }

        private static CancellationToken CreateTimingOutCancellationToken()
        {
            var source = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            return source.Token;
        }

        private static void UseConnection(Action<MongoStorage> action)
        {
            using (var storage = ConnectionUtils.CreateStorage())
            {
                action(storage);
            }
        }

        private static void UseConnection(Action<HangfireDbContext, MongoJobQueue> action)
        {
            using (var storage = ConnectionUtils.CreateStorage())
            using (var queue = new MongoJobQueue(storage, new MongoStorageOptions()))
            {
                action(storage.Connection, queue);
            }
        }
    }
}