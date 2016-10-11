using System;
using System.Collections.Generic;

namespace Hangfire.Mongo
{
    internal static class DictionaryExtensions
    {

        /// <summary>
        /// Gets the value associated with the specified key.
        /// </summary>
        /// <typeparam name="TKey">Key type</typeparam>
        /// <typeparam name="TValue">Value type</typeparam>
        /// <param name="dict">Dictionary</param>
        /// <param name="key">Key</param>
        /// <param name="defaultValue">Default value</param>
        /// <returns>Value for specified <paramref name="key"/>, or <paramref name="defaultValue"/> if no such key present.</returns>
        public static TValue TryGetValue<TKey, TValue>(this IDictionary<TKey, TValue> dict, TKey key, TValue defaultValue = default(TValue))
        {
            TValue value;
            if (dict == null || !dict.TryGetValue(key, out value))
                value = defaultValue;

            return value;
        }

        /// <summary>
        /// Gets the value associated with the specified key.
        /// </summary>
        /// <typeparam name="TKey">Key type</typeparam>
        /// <typeparam name="TValue">Value type</typeparam>
        /// <param name="dict">Dictionary</param>
        /// <param name="key">Key</param>
        /// <param name="defaultValueFactory">Default value factory</param>
        /// <returns>Value for specified <paramref name="key"/>, or result of <paramref name="defaultValueFactory"/> if no such key present.</returns>
        public static TValue TryGetValue<TKey, TValue>(this IDictionary<TKey, TValue> dict, TKey key, Func<TKey, TValue> defaultValueFactory)
        {
            TValue value;
            if (dict == null || !dict.TryGetValue(key, out value))
                value = defaultValueFactory(key);

            return value;
        }
        
        /// <summary>
        /// Gets the value associated with the specified key.
        /// </summary>
        /// <typeparam name="TKey">Key type</typeparam>
        /// <typeparam name="TValue">Value type</typeparam>
        /// <param name="dict">Dictionary</param>
        /// <param name="key">Key</param>
        /// <param name="defaultValueFactory">Default value factory</param>
        /// <returns>Value for specified <paramref name="key"/>, or result of <paramref name="defaultValueFactory"/> if no such key present.</returns>
        public static TValue TryGetValue<TKey, TValue>(this IDictionary<TKey, TValue> dict, TKey key, Func<IDictionary<TKey, TValue>, TKey, TValue> defaultValueFactory)
        {
            TValue value;
            if (dict == null || !dict.TryGetValue(key, out value))
                value = defaultValueFactory(dict, key);

            return value;
        }

    }
}
