using System.Threading;
using Hangfire.Storage;

namespace Hangfire.Mongo.PersistentJobQueue
{
    internal interface IPersistentJobQueue
    {
        IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken);

        void Enqueue(string queue, string jobId);

        void NotifyQueueChanged();
    }
}