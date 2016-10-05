using System;
using System.Linq;
using System.Threading;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.MongoUtils;
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
                var jobQueue = new JobQueueDto
                {
                    JobId = ObjectId.GenerateNewId().ToString(),
                    Queue = "default"
                };

                connection.JobQueue.InsertOne(jobQueue);
                
                // Act
                var fetchedJob = (MongoFetchedJob)queue.Dequeue(DefaultQueues, CreateTimingOutCancellationToken());

                // Assert
                Assert.Equal(jobQueue.Id, fetchedJob.Id);
                Assert.Equal(jobQueue.JobId, fetchedJob.JobId);
                Assert.Equal("default", fetchedJob.Queue);
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldLeaveJobInTheQueue_ButSetItsFetchedAtValue()
        {
            // Arrange
            UseConnection((connection, queue) =>
            {
                var job = new JobDto
                {
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = connection.GetServerTimeUtc()
                };
                connection.Job.InsertOne(job);

                var jobQueue = new JobQueueDto
                {
                    JobId = job.Id,
                    Queue = "default"
                };
                connection.JobQueue.InsertOne(jobQueue);
                
                // Act
                var fetchedJob = queue.Dequeue(DefaultQueues, CreateTimingOutCancellationToken());

                // Assert
                Assert.NotNull(fetchedJob);

                var fetchedAt = connection.JobQueue.AsQueryable().Single(_ => _.JobId == fetchedJob.JobId).FetchedAt;

                Assert.NotNull(fetchedAt);
                Assert.True(fetchedAt > DateTime.UtcNow.AddMinutes(-1));
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldFetchATimedOutJobs_FromTheSpecifiedQueue()
        {
            // Arrange
            UseConnection((connection, queue) =>
            {
                var job = new JobDto
                {
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = connection.GetServerTimeUtc()
                };
                connection.Job.InsertOne(job);

                var jobQueue = new JobQueueDto
                {
                    JobId = job.Id,
                    Queue = "default",
                    FetchedAt = connection.GetServerTimeUtc().AddDays(-1)
                };
                connection.JobQueue.InsertOne(jobQueue);
                
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
                var job1 = new JobDto
                {
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = connection.GetServerTimeUtc()
                };
                connection.Job.InsertOne(job1);

                var job2 = new JobDto
                {
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = connection.GetServerTimeUtc()
                };
                connection.Job.InsertOne(job2);

                connection.JobQueue.InsertOne(new JobQueueDto
                {
                    JobId = job1.Id,
                    Queue = "default"
                });

                connection.JobQueue.InsertOne(new JobQueueDto
                {
                    JobId = job2.Id,
                    Queue = "default"
                });
                
                // Act
                var fetchedJob = queue.Dequeue(DefaultQueues, CreateTimingOutCancellationToken());

                // Assert
                var unfetchedJob = connection.JobQueue.AsQueryable().Single(_ => _.JobId != fetchedJob.JobId);
                
                Assert.Null(unfetchedJob.FetchedAt);
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldFetchJobs_OnlyFromSpecifiedQueues()
        {
            UseConnection((connection, queue) =>
            {
                var job1 = new JobDto
                {
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = connection.GetServerTimeUtc()
                };
                connection.Job.InsertOne(job1);

                connection.JobQueue.InsertOne(new JobQueueDto
                {
                    JobId = job1.Id,
                    Queue = "critical"
                });
                
                Assert.Throws<OperationCanceledException>(
                    () => queue.Dequeue(DefaultQueues, CreateTimingOutCancellationToken()));
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldFetchJobs_FromMultipleQueuesBasedOnQueuePriority()
        {
            UseConnection((connection, queue) =>
            {
                var criticalJob = new JobDto
                {
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = connection.GetServerTimeUtc()
                };
                connection.Job.InsertOne(criticalJob);

                var defaultJob = new JobDto
                {
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = connection.GetServerTimeUtc()
                };
                connection.Job.InsertOne(defaultJob);

                connection.JobQueue.InsertOne(new JobQueueDto
                {
                    JobId = defaultJob.Id,
                    Queue = "default"
                });

                connection.JobQueue.InsertOne(new JobQueueDto
                {
                    JobId = criticalJob.Id,
                    Queue = "critical"
                });
                
                var critical = (MongoFetchedJob)queue.Dequeue(new[] { "critical", "default" }, CreateTimingOutCancellationToken());
                Assert.Equal(criticalJob.Id, critical.JobId);
                Assert.Equal("critical", critical.Queue);

                var @default = (MongoFetchedJob)queue.Dequeue(new[] { "critical", "default" }, CreateTimingOutCancellationToken());
                Assert.Equal(defaultJob.Id, @default.JobId);
                Assert.Equal("default", @default.Queue);
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldFetchJobs_OnAllWaitingThreads_WhenNotified()
        {
            UseConnection((connection, queue) =>
            {
                var defaultJobId = ObjectId.GenerateNewId().ToString();
                var customJobId = ObjectId.GenerateNewId().ToString();

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
                var jobId = ObjectId.GenerateNewId().ToString();
                queue.Enqueue("default", jobId);

                var fetchedJob = connection.JobQueue.AsQueryable().Single();
                Assert.Equal(jobId, fetchedJob.JobId);
                Assert.Equal("default", fetchedJob.Queue);
                Assert.Null(fetchedJob.FetchedAt);
            });
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