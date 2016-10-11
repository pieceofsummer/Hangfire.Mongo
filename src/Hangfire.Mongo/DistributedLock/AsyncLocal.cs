using System;
using System.Runtime.Remoting.Messaging;

namespace Hangfire.Mongo.DistributedLock
{
    internal class AsyncLocal<T>
    {
        private readonly string _name;

        public AsyncLocal()
        {
            _name = Guid.NewGuid().ToString();
        }

        public T Value
        {
            get
            {
                var value = CallContext.LogicalGetData(_name);
                return value == null ? default(T) : (T)value;
            }
            set
            {
                CallContext.LogicalSetData(_name, value);
            }
        }
    }
    
}
