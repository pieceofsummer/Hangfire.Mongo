namespace Hangfire.Mongo.PersistentJobQueue
{
    internal class EnqueuedAndFetchedCountDto
    {
        public int? EnqueuedCount { get; set; }

        public int? FetchedCount { get; set; }
    }
}