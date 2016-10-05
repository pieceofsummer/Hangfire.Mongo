using System;
using System.Runtime.ExceptionServices;
using System.Threading;

namespace Hangfire.Mongo.Tests.Utils
{
    /// <summary>
    /// Starts a new thread to execute an <paramref name="action"/> in parallel, 
    /// re-throws unhandled exceptions on the main thread upon completion.
    /// </summary>
    /// <example>
    /// This class is designed to be used in conjunction with <c>using</c> block:
    /// <code lang="cs">
    /// using (new <see cref="ParallelThread"/>(delegate
    /// {
    ///     OtherThreadWork();
    /// }))
    /// {
    ///     MainThreadWork();
    /// }
    /// 
    /// // will get here when both MainThreadWork() and OtherThreadWork() are finished
    /// </code>
    /// </example>
    public sealed class ParallelThread : IDisposable
    {
        private readonly Action _action;
        private readonly Thread _thread;
        private ExceptionDispatchInfo _exception = null;

        private static void Worker(object arg)
        {
            var self = (ParallelThread)arg;
            try
            {
                self._action();
            }
            catch (Exception ex)
            {
                self._exception = ExceptionDispatchInfo.Capture(ex);
            }
        }

        /// <summary>
        /// Initializes and starts an instance of <see cref="ParallelThread"/>.
        /// </summary>
        /// <param name="action">Delegate to execute on a new thread</param>
        public ParallelThread(Action action)
        {
            if (action == null)
                throw new ArgumentNullException(nameof(action));

            _action = action;
            _thread = new Thread(Worker);
            _thread.Start(this);
        }

        /// <summary>
        /// Waits for <see cref="ParallelThread"/> to complete, throws an exception if needed.
        /// </summary>
        void IDisposable.Dispose()
        {
            _thread.Join();
            _exception?.Throw();
        }
    }

}
