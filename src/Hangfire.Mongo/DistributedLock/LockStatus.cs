using System;

namespace Hangfire.Mongo.DistributedLock
{
    internal struct LockStatus
    {
        public static readonly LockStatus Acquired = new LockStatus();
        
        public LockStatus(bool inProc, TimeSpan timeout)
        {
            IsOccupied = true;
            IsInProcess = inProc;
            Timeout = timeout < TimeSpan.Zero ? TimeSpan.Zero : timeout;
        }

        /// <summary>
        /// The lock was not acquired 
        /// </summary>
        public bool IsOccupied { get; }

        /// <summary>
        /// The lock is held by the same process
        /// </summary>
        public bool IsInProcess { get; }

        /// <summary>
        /// Lock heartbeat timeout
        /// </summary>
        public TimeSpan Timeout { get; }
    }
}
