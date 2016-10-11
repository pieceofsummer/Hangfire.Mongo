using System;
using System.Collections.Generic;
using System.Threading;

namespace Hangfire.Mongo.DistributedLock
{
    /// <summary>
    /// Event-based lock awaiter for locks held by the same process
    /// </summary>
    internal class InProcessLockAwaiter : ILockAwaiter
    {
        private static readonly Dictionary<string, WeakReference<InProcessLockAwaiter>> _awaiters
            = new Dictionary<string, WeakReference<InProcessLockAwaiter>>();

        private readonly string _resource;
        private readonly AutoResetEvent _event;
        
        private InProcessLockAwaiter(string resource)
        {
            if (string.IsNullOrEmpty(resource))
                throw new ArgumentNullException(nameof(resource));
            
            _resource = resource;
            _event = new AutoResetEvent(false);
        }
        
        /// <summary>
        /// Gets or creates an instance of <see cref="InProcessLockAwaiter"/> for specified resource name
        /// </summary>
        /// <param name="resource">Resource name</param>
        public static InProcessLockAwaiter GetAwaiter(string resource)
        {
            if (string.IsNullOrEmpty(resource))
                throw new ArgumentNullException(nameof(resource));

            InProcessLockAwaiter awaiter;

            lock (_awaiters)
            {
                WeakReference<InProcessLockAwaiter> weakRef;
                if (!_awaiters.TryGetValue(resource, out weakRef) || !weakRef.TryGetTarget(out awaiter))
                {
                    awaiter = new InProcessLockAwaiter(resource);

                    if (weakRef == null)
                    {
                        weakRef = new WeakReference<InProcessLockAwaiter>(awaiter);
                        _awaiters.Add(resource, weakRef);
                    }
                    else
                    {
                        weakRef.SetTarget(awaiter);
                    }
                }
            }

            return awaiter;
        }

        /// <summary>
        /// Signals one of the waiting locks the associated resource was released
        /// </summary>
        public void Signal()
        {
            _event.Set();
        }

        /// <summary>
        /// Resets the signalled state
        /// </summary>
        public void Reset()
        {
            _event.Reset();
        }

        /// <inheritdoc />
        public bool Wait(TimeSpan timeout)
        {
            if (timeout <= TimeSpan.Zero)
            {
                // no time left
                return false;
            }

            return _event.WaitOne(timeout);
        }
    }
}
