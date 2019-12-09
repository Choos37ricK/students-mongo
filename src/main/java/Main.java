import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonDocument;
import org.bson.Document;

public class Main {
    public static void main(String[] args) {
        MongoClient mongoClient = new MongoClient("127.0.0.1", 27017);

        MongoDatabase mongodb = mongoClient.getDatabase("local");

        MongoCollection<Document> mongoCollection = mongodb.getCollection("students");

        mongoCollection.drop();

        //import csv

        System.out.println("Total quantity of the students in db: " + mongoCollection.countDocuments());

        BsonDocument query = BsonDocument.parse("{age: {$gt: 40}}");
        System.out.println("Students quantity which age > 40 years: " + mongoCollection.countDocuments(query));

        System.out.println("Name of the youngest student: " + mongoCollection
                .find(BsonDocument.parse("{{name: 1}}"))
                .sort(BsonDocument.parse("{age: 1}"))
                .limit(1));

        System.out.println("Name of the youngest student: " + mongoCollection
                .find(BsonDocument.parse("{{courses: 1}}"))
                .sort(BsonDocument.parse("{age: -1}"))
                .limit(1));
    }
}
