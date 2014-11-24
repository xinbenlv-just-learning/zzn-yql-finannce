var sqlite3 = require('sqlite3').verbose();

var util = require("util");

var log4js = require('log4js');
var logger = log4js.getLogger();
logger.debug("Start!");
var tables = [{
    name: 'industry',
    columns :[{ name: 'name'}, { name: 'id'}]
}, {
    name: 'company',
    columns :[{ name: 'name'}, { name: 'symbol'}]
}];

var db = new sqlite3.Database('tmp.db');


db.serialize(function() {
    for (var i in tables) {
        var table = tables[i];
        db.run(util.format("DROP TABLE IF EXISTS %s;", table.name), function(error, status) {
            if (error) logger.error(error);
            if (this) logger.debug(JSON.stringify(this));
        });

        var columns = "";
        for (var c in tables[i].columns) {
            var col = tables[i].columns[c];
            columns += util.format("%s %s,", col.name, col.dataType || "string");
        }

        var query = util.format("CREATE TABLE %s (%s);", table.name, columns.substring(0, columns.length -1));
        logger.debug(query);
        db.run(query, function(error) {
            if (error) logger.error(error);
            if (this) logger.debug(JSON.stringify(this));
        });
    }

});

db.close();