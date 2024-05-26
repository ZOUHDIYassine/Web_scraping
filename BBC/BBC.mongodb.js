/* global use, db */
// MongoDB Playground
// Use Ctrl+Space inside a snippet or a string literal to trigger completions.

const database = 'BBC';
const collection = 'BBC_news';

// The current database to use.
use(database);

// Get the cursor
var cursor = db.getCollection('BBC_news').find({});

//Iterate over the cursor
cursor.forEach(function(doc) {
    printjson(doc);
});



//db.getCollection('BBC_news').deleteMany({})