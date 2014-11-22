/**
 * Author: Zainan Victor Zhou (zzn@zzn.im)
 * Licensed under MIT License.
 */
var YQL = require('yql');
var writeJSONToFile = function(myData, outputFilename) {
	var fs = require('fs');
	fs.writeFile(outputFilename, JSON.stringify(myData, null, 4), function(err) {
	    if(err) {
	      console.log(err);
	    } else {
	      console.log("JSON saved to " + outputFilename);
	    }
	}); 
}

var writeStringToFile = function(myStr, outputFilename) {
    var fs = require('fs');
    fs.writeFile(outputFilename, myStr, function(err) {
        if(err) {
          console.log(err);
        } else {
          console.log("Str saved to " + outputFilename);
        }
    }); 
}

var queryGetAllTickers = 'select * from yahoo.finance.industry where id in (select industry.id from yahoo.finance.sectors)'
new YQL(queryGetAllTickers).exec(function (error, response) {
    writeJSONToFile(response, "response.json");
    var tickers = [];
    var tickersCommaSeparated = "";
    var industries = response.query.results.industry;
    var industrieList = [];
    for (var i in industries) {
        var industry = industries[i];
        industrieList.push(industry.name);
        var companies = industry.company;
        for (var c in companies) {
            var company = companies[c];
            var symbol = company.symbol;
            tickers.push(symbol);
            tickersCommaSeparated += ("," + symbol);
        }
    }

    writeJSONToFile(industrieList, "industrieList.json");
    writeJSONToFile(tickers, "tickers.json");
    writeStringToFile(tickersCommaSeparated, "tickersCommaSeparated.txt");
});