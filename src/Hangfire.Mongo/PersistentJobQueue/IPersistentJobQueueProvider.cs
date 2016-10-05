using Hangfire.Mongo.Database;

namespace Hangfire.Mongo.PersistentJobQueue
{
    internal interface IPersistentJobQueueProvider
    {
        IPersistentJobQueue GetJobQueue();

        IPersistentJobQueueMonitoringApi GetJobQueueMonitoringApi();
    }
}