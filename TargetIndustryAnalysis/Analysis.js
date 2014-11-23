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

/**
 * function callback()
 */
var GetTickers = function(callback) {

    var queryGetAllTickers = 'select * from yahoo.finance.industry where id in (select industry.id from yahoo.finance.sectors)'
    new YQL(queryGetAllTickers).exec(function (error, response) {
        var tickers = [];
        var industries = response.query.results.industry;
        var industrieList = [];
        for (var i in industries) {
            var industry = industries[i];
            // Filtering:
            if (!(parseInt(industry.id)>=800 && parseInt(industry.id)<900)) continue;
            industrieList.push({id: industry.id, name: industry.name});
            var companies = industry.company;
            for (var c in companies) {
                var company = companies[c];
                var symbol = company.symbol;
                tickers.push(symbol);
            }
        }
        callback(tickers);
    });
}

var GetCash = function(tickers) {
    console.log("Finished Reading Ticker Lists, reading " + tickers.length + " tickers");

    var fetchData = function (tickersComma, fileName) {
        var query = "SELECT * FROM yahoo.finance.balancesheet WHERE symbol in (" + tickersComma.substring(0, tickersComma.length - 1) + ")";
        console.log(query);
        new YQL(query).exec(function (error, response) {
            console.log("error, response");
            console.log(error);
            
            if (error) {
                console.log(error);
            } else {
                writeJSONToFile(response, fileName);
            }
        });
    }
    var tickersComma = "";

    for (var i in tickers) {
        var ticker = tickers[i];
        tickersComma += "'" + ticker + "',";
        if (i % 200 == 0) {
            console.log("fetch data for i = " + i);
            fetchData(tickersComma, "cash" + i + ".json");
            tickersComma = ""; // clean up
        }
    }
    fetchData(tickersComma, "cashLast.json"); // Last parts
}

GetSector800(GetCash);
