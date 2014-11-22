/**
 * Author: Zainan Victor Zhou (zzn@zzn.im)
 * Licensed under MIT License.
 */
var YQL = require('yql');
var query = 'select * from yahoo.finance.industry where id in (select industry.id from yahoo.finance.sectors)'
var yql = new YQL(query);
var writeToFile = function(myData, outputFilename) {
	var fs = require('fs');
	fs.writeFile(outputFilename, JSON.stringify(myData, null, 4), function(err) {
	    if(err) {
	      console.log(err);
	    } else {
	      console.log("JSON saved to " + outputFilename);
	    }
	}); 
}
yql.exec(function (error, response) {
    console.log(JSON.stringify(response, null, 4));
    writeToFile(response, "response.json");
    var tickers = [];
    var industries = response.query.results.industry;
    for (var i in industries) {
    	var industry = industries[i];
    	var companies = industry.company;
    	for (var c in companies) {
    		var company = companies[c];
    		var symbol = company.symbol;
    		tickers.push(symbol);
    	}
    }
    writeToFile(tickers, "ickers.json");
});

