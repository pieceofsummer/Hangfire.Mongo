using System;
using Hangfire.Annotations;

namespace Hangfire.Mongo.PersistentJobQueue.Mongo
{
    internal class MongoJobQueueProvider : IPersistentJobQueueProvider, IDisposable
    {
        private readonly MongoJobQueue _queue;
        private readonly MongoJobQueueMonitoringApi _monitoringApi;

        public MongoJobQueueProvider([NotNull] MongoStorage storage, [NotNull] MongoStorageOptions options)
        {
            if (storage == null)
                throw new ArgumentNullException(nameof(storage));
            if (options == null)
                throw new ArgumentNullException(nameof(options));

            _queue = new MongoJobQueue(storage, options);
            _monitoringApi = new MongoJobQueueMonitoringApi(storage);
        }

        public IPersistentJobQueue GetJobQueue() => _queue;

        public IPersistentJobQueueMonitoringApi GetJobQueueMonitoringApi() => _monitoringApi;

        public void Dispose()
        {
            _queue.Dispose();
        }
    }
}