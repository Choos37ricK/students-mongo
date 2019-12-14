import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.bson.BsonDocument;
import org.bson.Document;

import org.apache.commons.csv.CSVParser;

import java.io.*;
import java.util.*;
import java.util.function.Consumer;

public class Main {
    public static void main(String[] args) {
        MongoClient mongoClient = new MongoClient("127.0.0.1", 27017);

        MongoDatabase mongodb = mongoClient.getDatabase("local");

        MongoCollection<Document> mongoCollection = mongodb.getCollection("students");

        mongoCollection.drop();

        File file = new File("mongo.csv");
        try {
            Reader reader = new BufferedReader(new FileReader(file));
            CSVParser parser = new CSVParser(reader, CSVFormat.DEFAULT.withDelimiter(','));
            Iterator<CSVRecord> iterator = parser.iterator();

            Map<String, Object> map;

            int id = 1;
            while (iterator.hasNext()) {
                CSVRecord next = iterator.next();
                map = new HashMap<>();
                map.put("_id", id);
                map.put("name", next.get(0).trim());
                map.put("age", Integer.parseInt(next.get(1).trim()));

                ArrayList<String> coursesList = new ArrayList<>(Arrays.asList(next.get(2).trim().split(",")));
                map.put("courses", coursesList);
                Document student = new Document(map);

                mongoCollection.insertOne(student);
                id++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("Total quantity of the students in db: " + mongoCollection.countDocuments());

        BsonDocument query = BsonDocument.parse("{age: {$gt: 40}}");
        System.out.println("Students quantity which age > 40 years: " + mongoCollection.countDocuments(query));

        mongoCollection
                .find(BsonDocument.parse("{}, {name: 1, _id: 0}"))
                .sort(BsonDocument.parse("{age: 1}"))
                .limit(1)
                .forEach((Consumer<Document>) document -> System.out.println("Name of the youngest student: " + document.get("name")));

        mongoCollection
                .find(BsonDocument.parse("{}, {courses: 1, _id: 0}"))
                .sort(BsonDocument.parse("{age: -1}"))
                .limit(1)
                .forEach((Consumer<Document>) document -> System.out.println("Courses of the oldest student: " + document.get("courses")));
    }
}
