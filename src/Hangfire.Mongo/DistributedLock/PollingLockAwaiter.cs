using System;
using System.Threading;

namespace Hangfire.Mongo.DistributedLock
{
    /// <summary>
    /// Primitive poll-based lock awaiter
    /// </summary>
    internal struct PollingLockAwaiter : ILockAwaiter
    {
        private readonly TimeSpan _timeout;

        /// <summary>
        /// Initializes an instance of <see cref="PollingLockAwaiter"/> by estimating 
        /// wait interval based on lock acquisition timeout and lock heartbeat timeout
        /// </summary>
        /// <param name="acquireTimeout">Lock acquisition timeout</param>
        /// <param name="heartbeatTimeout">Lock heartbeat timeout</param>
        public PollingLockAwaiter(TimeSpan acquireTimeout, TimeSpan heartbeatTimeout)
        {
            // TODO: use some better timeout strategy

            if (acquireTimeout < heartbeatTimeout)
            {
                _timeout = TimeSpan.FromMilliseconds(acquireTimeout.TotalMilliseconds / 5);
            }
            else
            {
                _timeout = heartbeatTimeout;
            }
        }
        
        public bool Wait(TimeSpan timeout)
        {
            if (timeout > _timeout)
            {
                // wait no more than designated timeout
                timeout = _timeout;
            }

            if (timeout <= TimeSpan.Zero)
            {
                // no time left
                return false;
            }
            
            Thread.Sleep(timeout);

            // always return true to try acquire lock
            return true;
        }
    }
}
