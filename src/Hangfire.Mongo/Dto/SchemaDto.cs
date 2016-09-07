using MongoDB.Bson.Serialization.Attributes;

namespace Hangfire.Mongo.Dto
{
    [BsonIgnoreExtraElements]
    internal class SchemaDto
    {
        [BsonId]
        public int Version { get; set; }
    }
}