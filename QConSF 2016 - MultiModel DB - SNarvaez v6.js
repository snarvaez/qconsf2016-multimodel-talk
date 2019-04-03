// ========================
// Sig Narvaez
// MongoDB 3.4 Multi-model
// QCon San Francisco 2016
// "The Rise of the Multimodel Database"
// ========================

// items =========
db.items.drop();
db.items.insert({
	"sku": "ITEM123",
	"name": "MUG 123",
	"unit_cost": 15.95,
	"images": [{
		"front-view": "mug-front-view"
	}, {
		"left-view": "mug-left-view"
	}]
});
db.items.insert({
	"sku": "ITEM456",
	"name": "MUG 456",
	"unit_cost": 19.95,
	"images": [{
		"front-view": "mug-front-view"
	}, {
		"left-view": "mug-left-view"
	}]
});

// customers =========
db.customers.drop();
db.customers.insert({
	"customerID": "CUST456",
	"name": "Sig Narvaez",
	"adresses": [{
		"type": "home",
		"address": "123 MyStreet, MyCity, CA, 123456"
	}, {
		"type": "mail",
		"address": "456 MyMail, MyCity, CA, 123456"
	}]
});

// orders =========
db.orders.drop();
db.orders.insert({
	"orderID": "ORD123",
	"total": 3450.45,
	"customer": {
		"name": "Sig Narvaez",
		"customerID": "CUST456"
	},
	"items": [{
		"sku": "ITEM123",
		"name": "MUG 123",
		"qty": 3
	}, {
		"sku": "ITEM456",
		"name": "MUG 456",
		"qty": 5
	}],
	"payment": {
		"method": "visa",
		"txID": "123123XXXDD11DD"
	}
});



// $lookup =========
db.orders.aggregate([
  { "$unwind"  : "$items"},
  { "$lookup"  : {
       "from"        : "items",
       "localField"  : "items.sku",
       "foreignField": "sku",
       "as"          : "items_full" }},
  { "$unwind"  : "$items_full" },
	{ "$group" : {  "_id"   : "$orderID",
									"total" : { $sum: { $multiply: [ "$items.qty", "$items_full.unit_cost" ] } }
								}}
]);

// view with $lookup =========
db.vwRecalcOrderTotals.drop();
db.createView("vwRecalcOrderTotals", "orders",
	[ { "$unwind"  : "$items"},
	  { "$lookup"  : {
	       "from"        : "items",
	       "localField"  : "items.sku",
	       "foreignField": "sku",
	       "as"          : "items_full" }},
	  { "$unwind"  : "$items_full" },
		{ "$group" : {  "_id"   : "$orderID",
										"total" : { $sum: { $multiply: [ "$items.qty", "$items_full.unit_cost" ] } }
									}}
	]
);
db.vwRecalcOrderTotals.find();

// schema validation

// Both rules are valid
db.runCommand({collMod: "orders",
	validator : {
		"$and"  : [
			{"orderID" : {"$type" : "string"}},
			{"customer.customerID" : {"$type": "string"}},
			{"$expr" : {"$gte" : [ {"$size":"$items"}, 1]}}
    ]},
		"validationLevel": "strict",
		"validationAction": "error"
});

db.runCommand({collMod: "orders",
	validator : {
		"$and"  : [
			{"orderID" : {"$type" : "string"}},
			{"customer.customerID" : {"$type": "string"}},
			{"items.0" : {"$exists" : true}}
    ]},
		"validationLevel": "strict",
		"validationAction": "error"
});

// Fails
db.orders.insert({});

// Fails
db.orders.insert({
	"orderID": "order1",
	"customer": {"customerID": "customer1"},
	"items": []
});

// Passes
db.orders.insert({
	"orderID": "order1",
	"customer": {"customerID": "customer1"},
	"items": [{
		"sku": "ITEM123",
		"name": "MUG 123",
		"qty": 3
	}, {
		"sku": "ITEM456",
		"name": "MUG 456",
		"qty": 5
	}],
	"payment": {
		"method": "visa",
		"txID": "ABC123XYZDD11DD"
	}
});


// Facets
db.itemsFacets.drop()
db.itemsFacets.insert({ _id: 1, title: "Lightsaber Mug", "artist" : "Grosz", year: 1977, tags: [ "Star Wars", "Sci-Fi", "Geek", "Movies" ] })
db.itemsFacets.insert({ _id: 2, title: "Chewbacca Mug", "artist" : "Munch", year: 1977, tags: [ "Star Wars", "Sci-Fi", "Geek", "Movies", "Characters" ] })
db.itemsFacets.insert({ _id: 3, title: "Data Mug", "artist" : "Miro", year: 1987, tags: [ "Star Trek", "Sci-Fi", "Geek", "TV", "Characters" ] })
db.itemsFacets.insert({ _id: 4, title: "Captain Pickard Mug", artist: "Hokusai", year: 1987, tags: [ "Star Trek", "Sci-Fi", "Geek", "TV", "Characters" ] })
db.itemsFacets.insert({ _id: 5, title: "Optimus Prime Mug", artist: "Cullen", tags: [ "Transformers", "Cartoons", "Geek", "TV", "Characters" ] })
db.itemsFacets.insert({ _id: 6, title: "Megatron Mug", artist: "Welker", tags: [ "Transformers", "Cartoons", "Geek", "TV", "Characters" ] })

db.itemsFacets.aggregate( [
   {
      $facet: {
         "categorizedByTags": [  { $unwind: "$tags" },  { $sortByCount: "$tags" } ],
         "categorizedByYears": [
            { $match: { year: {$exists: 1 } } },
            { $bucket: { groupBy: "$year", boundaries: [ 1970, 1980, 1990 ] } }
         ],
         "categorizedByYears(Auto)": [ { $bucketAuto: { groupBy: "$year", buckets: 4 } } ]
      }
   }
]).pretty();

// Facets
db.mugsFacets.drop()
db.mugsFacets.insert({ _id: 1, title: "MongoSV Mug", year: 2015, tags: [ "MongoDB Days", "California", "NA" ] })
db.mugsFacets.insert({ _id: 2, title: "MongoOC Mug", year: 2012, tags: [ "MongoDB User Group", "California", "NA" ] })
db.mugsFacets.insert({ _id: 3, title: "MongoDallas Mug", year: 2013, tags: [ "MongoDB User Group", "TOLA Region", "NA" ] })
db.mugsFacets.insert({ _id: 4, title: "MongoDublin Mug", year: 2012, tags: [ "MongoDB Days", "Ireland", "EMEA"] })
db.mugsFacets.insert({ _id: 5, title: "MongoParis Mug", year: 2008, tags: [ "MongoDB Days", "France", "EMEA" ] })
db.mugsFacets.insert({ _id: 6, title: "Mongo Gray Mug", year: 2007, tags: [ "MongoDB User Group", "New York", "NA" ] })

db.mugsFacets.aggregate( [
   {
      $facet: {
         "categorizedByTags": [  { $unwind: "$tags" },  { $sortByCount: "$tags" } ],
         "categorizedByYears": [
            { $match: { year: {$exists: 1 } } },
            { $bucket: { groupBy: "$year", boundaries: [ 2000, 2010, 2016 ] } } //2015
         ],
         "categorizedByYears(Auto)": [ { $bucketAuto: { groupBy: "$year", buckets: 2 } } ]
      }
   }
]).pretty();

db.categoryGraph.drop();
db.categoryGraph.insertMany([
  { _id: 1, name: "Mugs" },
  { _id: 2, name: "Kitchen & Dining", parentId: 1},
  { _id: 3, name: "Commuter & Travel", parentId: 2},
  { _id: 4, name: "Glassware & Drinkware", parentId: 2},
  { _id: 5, name: "Outdoor Recreation", parentId: 1},
  { _id: 6, name: "Camping Mugs", parentId: 5},
  { _id: 7, name: "Running Thermos", parentId: 5},
	{ _id: 8, name: "Red Run Thermos", parentId: 7},
  { _id: 9, name: "White Run Thermos", parentId: 7},
	{ _id:10, name: "Blue Run Thermos", parentId: 7},
]);

db.vwCategoryDepth1.drop();
db.createView("vwCategoryDepth1", "categoryGraph", [
	{ $graphLookup: {
				from:             "categoryGraph",
		    startWith:        "$_id",
		    connectFromField: "_id",
		    connectToField:   "parentId",
				maxDepth:         0, // <=======
		    as:               "Children" }}
]);

db.vwCategoryDepth2.drop();
db.createView("vwCategoryDepth2", "categoryGraph", [
	{ $graphLookup: {
				from:             "categoryGraph",
		    startWith:        "$_id",
		    connectFromField: "_id",
		    connectToField:   "parentId",
				maxDepth:         1, // <======
		    as:               "Children" }}
]);

db.categoryGraph.aggregate([
  { $match:       { _id: 1 }}, // <======
  { $graphLookup: {
				from:             "vwCategoryDepth1",
		    startWith:        "$_id",
		    connectFromField: "_id",
		    connectToField:   "parentId",
				maxDepth:         0,
		    as:               "Children" }}
]).pretty();

db.categoryGraph.aggregate([
  { $match:       { _id: 5 }}, // <======
  { $graphLookup: {
				from:             "vwCategoryDepth1",
		    startWith:        "$_id",
		    connectFromField: "_id",
		    connectToField:   "parentId",
				maxDepth:         0,
		    as:               "Children" }}
]).pretty();

// IS THIS POSSIBLE? - INCEPTION!!!
db.createView("vwCategoryDepthSelf", "categoryGraph", [
	{ $graphLookup: {
				from:             "vwCategoryDepthSelf",
		    startWith:        "$_id",
		    connectFromField: "_id",
		    connectToField:   "parentId",
				depthField:       "depth",
		    as:               "Children" }}
]);
