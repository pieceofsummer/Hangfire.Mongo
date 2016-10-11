using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;

namespace Hangfire.Mongo.Database
{
    internal static class CollectionExtensions
    {
        /// <summary>
        /// Shortcut to return all documents from collection
        /// </summary>
        /// <typeparam name="TDocument">Document type</typeparam>
        /// <param name="collection">Collection</param>
        /// <param name="options">Find options</param>
        public static IFindFluent<TDocument, TDocument> AllDocuments<TDocument>(
            this IMongoCollection<TDocument> collection, FindOptions options = null)
        {
            return collection.Find(Builders<TDocument>.Filter.Empty, options);
        }

        /// <summary>
        /// Shortcut to count all documents in collection
        /// </summary>
        /// <typeparam name="TDocument">Document type</typeparam>
        /// <param name="collection">Collection</param>
        /// <param name="options">Count options</param>
        public static long Count<TDocument>(this IMongoCollection<TDocument> collection, CountOptions options = null)
        {
            return collection.Count(Builders<TDocument>.Filter.Empty, options);
        }

        /// <summary>
        /// Shortcut to return documents as dictionary
        /// </summary>
        /// <typeparam name="TDocument">Document type</typeparam>
        /// <typeparam name="TKey">Dictionary key</typeparam>
        /// <typeparam name="TValue">Dictionary value</typeparam>
        /// <param name="aggregate">Aggregated collection</param>
        /// <param name="keySelector">Key selector</param>
        /// <param name="valueSelector">Value selector</param>
        public static Dictionary<TKey, TValue> ToDictionary<TDocument, TKey, TValue>(this IAggregateFluent<TDocument> aggregate, 
            Func<TDocument, TKey> keySelector, Func<TDocument, TValue> valueSelector)
        {
            return aggregate.ToEnumerable().ToDictionary(keySelector, valueSelector);
        }

        /// <summary>
        /// Shortcut to return documents as dictionary
        /// </summary>
        /// <typeparam name="TDocument">Document type</typeparam>
        /// <typeparam name="TProjection">Projection type</typeparam>
        /// <typeparam name="TKey">Dictionary key</typeparam>
        /// <typeparam name="TValue">Dictionary value</typeparam>
        /// <param name="results">Find results</param>
        /// <param name="keySelector">Key selector</param>
        /// <param name="valueSelector">Value selector</param>
        public static Dictionary<TKey, TValue> ToDictionary<TDocument, TProjection, TKey, TValue>(this IFindFluent<TDocument, TProjection> results,
            Func<TProjection, TKey> keySelector, Func<TProjection, TValue> valueSelector)
        {
            return results.ToEnumerable().ToDictionary(keySelector, valueSelector);
        }
    }
}
