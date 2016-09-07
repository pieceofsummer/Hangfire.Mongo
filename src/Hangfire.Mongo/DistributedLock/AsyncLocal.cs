using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Hangfire.Mongo.DistributedLock
{
#if NET45

    using System.Runtime.Remoting.Messaging;

    internal class AsyncLocal<T>
    {
        private readonly string __name;

        public AsyncLocal()
        {
            __name = Guid.NewGuid().ToString();
        }

        public T Value
        {
            get
            {
                var value = CallContext.LogicalGetData(__name);
                return value == null ? default(T) : (T)value;
            }
            set
            {
                CallContext.LogicalSetData(__name, value);
            }
        }
    }

#endif
}
