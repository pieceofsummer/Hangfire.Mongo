using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Hangfire.Mongo.PersistentJobQueue
{
    internal class PersistentJobQueueProviderCollection : IEnumerable<IPersistentJobQueueProvider>, IDisposable
    {
        private readonly List<IPersistentJobQueueProvider> _providers 
            = new List<IPersistentJobQueueProvider>();

        private readonly Dictionary<string, IPersistentJobQueueProvider> _providersByQueue 
            = new Dictionary<string, IPersistentJobQueueProvider>(StringComparer.OrdinalIgnoreCase);

        private readonly IPersistentJobQueueProvider _defaultProvider;

        public PersistentJobQueueProviderCollection(IPersistentJobQueueProvider defaultProvider)
        {
            if (defaultProvider == null) throw new ArgumentNullException(nameof(defaultProvider));

            _defaultProvider = defaultProvider;

            _providers.Add(_defaultProvider);
        }

        public void Add(IPersistentJobQueueProvider provider, IEnumerable<string> queues)
        {
            if (provider == null)
                throw new ArgumentNullException(nameof(provider));
            if (queues == null)
                throw new ArgumentNullException(nameof(queues));

            _providers.Add(provider);

            foreach (var queue in queues)
            {
                _providersByQueue.Add(queue, provider);
            }
        }

        public IPersistentJobQueueProvider GetProvider(string queue)
        {
            return _providersByQueue.TryGetValue(queue, _defaultProvider);
        }

        public IEnumerator<IPersistentJobQueueProvider> GetEnumerator()
        {
            return _providers.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public void Dispose()
        {
            foreach (var disposable in _providers.OfType<IDisposable>())
            {
                disposable.Dispose();
            }
        }
    }
}