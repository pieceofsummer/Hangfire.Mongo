using System;

namespace Hangfire.Mongo.DistributedLock
{
    /// <summary>
    /// A no-op distributed lock
    /// </summary>
    internal class NoOpDistributedLock : IDisposable
    {
        public void Dispose()
        {
        }
    }
}
