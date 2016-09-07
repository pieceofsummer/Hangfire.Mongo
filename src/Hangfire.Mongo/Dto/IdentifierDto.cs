using MongoDB.Bson.Serialization.Attributes;

namespace Hangfire.Mongo.Dto
{
    internal class IdentifierDto
    {
        [BsonId]
        public string Id { get; set; }

        [BsonElement("seq")]
        public long Seq { get; set; }
    }
}
