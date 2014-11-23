/**
 * Author: Zainan Victor Zhou (zzn@zzn.im)
 * Licensed under MIT License.
 * This Script try to find the following C
 */

var log4js = require('log4js');
var logger = log4js.getLogger();
logger.debug("Start!");
var async = require('async');
var Q = require("q");
var balancesheets = [];
function writeJSONToFile(myData, outputFilename) {
	var fs = require('fs');
	fs.writeFile(outputFilename, JSON.stringify(myData, null, 4), function(err) {
	    if(err) {
	      console.log(err);
	    } else {
	      console.log("JSON saved to " + outputFilename);
	    }
	}); 
}

function writeStringToFile(myStr, outputFilename) {
    var fs = require('fs');
    fs.writeFile(outputFilename, myStr, function(err) {
        if(err) {
          console.log(err);
        } else {
          console.log("Str saved to " + outputFilename);
        }
    }); 
}

Array.prototype.chunk = function(chunkSize) {
    var array=this;
    return [].concat.apply([],
        array.map(function(elem,i) {
            return i%chunkSize ? [] : [array.slice(i,i+chunkSize)];
        })
    );
}

function GetQuery(tickers) {
    var tickersComma = "";
    for (var i in tickers) {
        var ticker = tickers[i];
        tickersComma += "'" + ticker +"',";
    }
    return "SELECT * FROM yahoo.finance.balancesheet WHERE symbol in (" + tickersComma.substring(0, tickersComma.length - 1) + ")";
};

var YQL = require('yqlp');

var query = 'select * from yahoo.finance.industry where id in (select industry.id from yahoo.finance.sectors)';
YQL.execp(query)
.then(function(response) {
    logger.debug("Finished first query");
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
    //logger.debug(tickers);
    return tickers;
})
.then(function(tickers) {
    logger.debug("Getting " + tickers.length + " tickers.");
    var promises = [];
    tickers.chunk(200).forEach(function(tickers){
        promises.push(Q.fcall(function () { 
            var query = GetQuery(tickers);
            return YQL.execp(query).then(function(response) {
                return response;
            });
            
        }));
    });

    return Q.allSettled(promises).then(function (results) {
        logger.debug("AllSettled");
        logger.debug(results);
        results.forEach(function (result) {
            logger.debug("Each settled");
            if (result.state === "fulfilled") {
                var value = result.value;
                balancesheets.push(value);
            } else {
                var reason = result.reason;
            }

        });
        writeJSONToFile(balancesheets, "balancesheets.json");
    });        
})
.catch(function (error) {
    console.log(error);
});

