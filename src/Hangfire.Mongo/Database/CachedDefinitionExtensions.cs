using System;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;

namespace Hangfire.Mongo.Database
{
    internal static class CachedDefinitionExtensions
    {
        /// <summary>
        /// Creates a caching wrapper around <see cref="FieldDefinition{TDocument}"/>
        /// </summary>
        /// <typeparam name="TDocument">Document type</typeparam>
        /// <param name="field">Source field definition</param>
        public static FieldDefinition<TDocument> Cached<TDocument>(this FieldDefinition<TDocument> field)
        {
            return new CachedFieldDefinition<TDocument>(field);
        }

        private sealed class CachedFieldDefinition<TDocument> : FieldDefinition<TDocument>
        {
            private FieldDefinition<TDocument> _inner;
            private RenderedFieldDefinition _cached;

            public CachedFieldDefinition(FieldDefinition<TDocument> inner)
            {
                _inner = inner;
                _cached = null;
            }

            public override RenderedFieldDefinition Render(IBsonSerializer<TDocument> documentSerializer, IBsonSerializerRegistry serializerRegistry)
            {
                if (_cached == null)
                {
                    lock (this)
                    {
                        if (_cached == null)
                        {
                            _cached = _inner.Render(documentSerializer, serializerRegistry);
                            _inner = null; // release ref
                        }
                    }
                }

                return _cached;
            }
        }
        
        /// <summary>
        /// Creates a caching wrapper around <see cref="FieldDefinition{TDocument}"/>
        /// </summary>
        /// <typeparam name="TDocument">Document type</typeparam>
        /// <typeparam name="TField">Field type</typeparam>
        /// <param name="field">Source field definition</param>
        public static FieldDefinition<TDocument, TField> Cached<TDocument, TField>(this FieldDefinition<TDocument, TField> field)
        {
            return new CachedFieldDefinition<TDocument, TField>(field);
        }

        private sealed class CachedFieldDefinition<TDocument, TField> : FieldDefinition<TDocument, TField>
        {
            private FieldDefinition<TDocument, TField> _inner;
            private RenderedFieldDefinition<TField> _cached;

            public CachedFieldDefinition(FieldDefinition<TDocument, TField> inner)
            {
                _inner = inner;
                _cached = null;
            }

            public override RenderedFieldDefinition<TField> Render(IBsonSerializer<TDocument> documentSerializer, IBsonSerializerRegistry serializerRegistry)
            {
                if (_cached == null)
                {
                    lock (this)
                    {
                        if (_cached == null)
                        {
                            _cached = _inner.Render(documentSerializer, serializerRegistry);
                            _inner = null; // release ref
                        }
                    }
                }

                return _cached;
            }
        }
        
        /// <summary>
        /// Creates a caching wrapper around <see cref="FilterDefinition{TDocument}"/>
        /// </summary>
        /// <typeparam name="TDocument">Document type</typeparam>
        /// <param name="filter">Source filter definition</param>
        public static FilterDefinition<TDocument> Cached<TDocument>(this FilterDefinition<TDocument> filter)
        {
            return new CachedFilterDefinition<TDocument>(filter);
        }
        
        private sealed class CachedFilterDefinition<TDocument> : FilterDefinition<TDocument>
        {
            private FilterDefinition<TDocument> _inner;
            private BsonDocument _cached;

            public CachedFilterDefinition(FilterDefinition<TDocument> inner)
            {
                _inner = inner;
                _cached = null;
            }

            public override BsonDocument Render(IBsonSerializer<TDocument> documentSerializer, IBsonSerializerRegistry serializerRegistry)
            {
                if (_cached == null)
                {
                    lock (this)
                    {
                        if (_cached == null)
                        {
                            _cached = _inner.Render(documentSerializer, serializerRegistry);
                            _inner = null; // release ref
                        }
                    }
                }

                return _cached;
            }
        }
        
        /// <summary>
        /// Creates a caching wrapper around <see cref="ProjectionDefinition{TSource}"/>
        /// </summary>
        /// <typeparam name="TDocument">Document type</typeparam>
        /// <param name="projection">Source projection definition</param>
        public static ProjectionDefinition<TDocument> Cached<TDocument>(this ProjectionDefinition<TDocument> projection)
        {
            return new CachedProjectionDefinition<TDocument>(projection);
        }

        private sealed class CachedProjectionDefinition<TDocument> : ProjectionDefinition<TDocument>
        {
            private ProjectionDefinition<TDocument> _inner;
            private BsonDocument _cached;

            public CachedProjectionDefinition(ProjectionDefinition<TDocument> inner)
            {
                _inner = inner;
                _cached = null;
            }

            public override BsonDocument Render(IBsonSerializer<TDocument> sourceSerializer, IBsonSerializerRegistry serializerRegistry)
            {
                if (_cached == null)
                {
                    lock (this)
                    {
                        if (_cached == null)
                        {
                            _cached = _inner.Render(sourceSerializer, serializerRegistry);
                            _inner = null; // release ref
                        }
                    }
                }

                return _cached;
            }
        }
        
        /// <summary>
        /// Creates a caching wrapper around <see cref="ProjectionDefinition{TSource, TProjection}"/>
        /// </summary>
        /// <typeparam name="TDocument">Document type</typeparam>
        /// <typeparam name="TProjection">Projection type</typeparam>
        /// <param name="projection">Source projection definition</param>
        public static ProjectionDefinition<TDocument, TProjection> Cached<TDocument, TProjection>(this ProjectionDefinition<TDocument, TProjection> projection)
        {
            return new CachedProjectionDefinition<TDocument, TProjection>(projection);
        }
        
        private sealed class CachedProjectionDefinition<TDocument, TProjection> : ProjectionDefinition<TDocument, TProjection>
        {
            private ProjectionDefinition<TDocument, TProjection> _inner;
            private RenderedProjectionDefinition<TProjection> _cached;

            public CachedProjectionDefinition(ProjectionDefinition<TDocument, TProjection> inner)
            {
                _inner = inner;
                _cached = null;
            }

            public override RenderedProjectionDefinition<TProjection> Render(IBsonSerializer<TDocument> sourceSerializer, IBsonSerializerRegistry serializerRegistry)
            {
                if (_cached == null)
                {
                    lock (this)
                    {
                        if (_cached == null)
                        {
                            _cached = _inner.Render(sourceSerializer, serializerRegistry);
                            _inner = null; // release ref
                        }
                    }
                }

                return _cached;
            }
        }
        
        /// <summary>
        /// Creates a caching wrapper around <see cref="SortDefinition{TDocument}"/>
        /// </summary>
        /// <typeparam name="TDocument">Document type</typeparam>
        /// <param name="sort">Source sort definition</param>
        public static SortDefinition<TDocument> Cached<TDocument>(this SortDefinition<TDocument> sort)
        {
            return new CachedSortDefinition<TDocument>(sort);
        }

        private sealed class CachedSortDefinition<TDocument> : SortDefinition<TDocument>
        {
            private SortDefinition<TDocument> _inner;
            private BsonDocument _cached;

            public CachedSortDefinition(SortDefinition<TDocument> inner)
            {
                _inner = inner;
                _cached = null;
            }

            public override BsonDocument Render(IBsonSerializer<TDocument> documentSerializer, IBsonSerializerRegistry serializerRegistry)
            {
                if (_cached == null)
                {
                    lock (this)
                    {
                        if (_cached == null)
                        {
                            _cached = _inner.Render(documentSerializer, serializerRegistry);
                            _inner = null; // release ref
                        }
                    }
                }

                return _cached;
            }
        }
    }
}
