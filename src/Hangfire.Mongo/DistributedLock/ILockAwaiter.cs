using System;

namespace Hangfire.Mongo.DistributedLock
{
    /// <summary>
    /// Base interface for lock awaiters
    /// </summary>
    internal interface ILockAwaiter
    {
        /// <summary>
        /// Waits for the next attempt the lock can be acquired.
        /// </summary>
        /// <param name="timeout">Timeout</param>
        /// <returns>Returns <c>true</c> if lock can be acquired; <c>false</c> otherwise</returns>
        bool Wait(TimeSpan timeout);
    }
}
