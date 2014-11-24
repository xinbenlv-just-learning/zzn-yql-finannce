/**
 * Author: Zainan Victor Zhou (zzn@zzn.im)
 * Licensed under MIT License.
 * This Script try to find the following C
 */

var log4js = require('log4js');
var logger = log4js.getLogger();
logger.debug("Start!");
var Q = require("q");
var balancesheets = [];
var companyProfiles = {};
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

function GetBalanceSheetQuery(tickers) {
    var tickersComma = "";
    for (var i in tickers) {
        var ticker = tickers[i];
        tickersComma += "'" + ticker +"',";
    }
    return "SELECT * FROM yahoo.finance.balancesheet WHERE symbol in (" + tickersComma.substring(0, tickersComma.length - 1) + ")";
};

function GetMarketPriceQuery(tickers) {
    var tickersComma = "";
    for (var i in tickers) {
        var ticker = tickers[i];
        tickersComma += "'" + ticker +"',";
    }
    return "SELECT * FROM yahoo.finance.keystats WHERE symbol in (" + tickersComma.substring(0, tickersComma.length - 1) + ")";
};

var YQL = require('yqlp');

var query = 'select * from yahoo.finance.industry where id in (select industry.id from yahoo.finance.sectors)';
YQL.execp(query)
.then(function(response) {
    logger.debug("Finished first query");
    var tickers = [];
    if ("results" in response.query) {
        var industries = response.query.results.industry;
    } else {
        throw new Error("results not in query, query:" + JSON.stringify(response));
    }
    var industrieList = [];
    for (var i in industries) {
        var industry = industries[i];
        // Filtering:
        //if (!(parseInt(industry.id)>=800 && parseInt(industry.id)<900)) continue;
        //if (parseInt(industry.id)!=812) continue;
        industrieList.push({id: industry.id, name: industry.name});
        var companies = industry.company;
        for (var c in companies) {
            var company = companies[c];
            if (typeof company == "object" && "symbol" in company) {
                tickers.push(company.symbol);
            } else {
                logger.warn("symbol not exist in " + company);
            }
        }
    }
    //logger.debug(tickers);
    return tickers;
})
.then(function(tickers) {
  var filteredTickers = [];
  for(var i in tickers) {
      var ticker = tickers[i];
      if (ticker.toString().indexOf(".") != -1) continue;
      filteredTickers.push(ticker);
  }
  return filteredTickers;
})
.then(function(tickers) {
    logger.debug("Getting " + tickers.length + " tickers.");
    var promises = [];
    Array.prototype.chunk = function(chunkSize) {
        var array=this;
        return [].concat.apply([],
            array.map(function(elem,i) {
                return i%chunkSize ? [] : [array.slice(i,i+chunkSize)];
            })
        );
    };
    tickers.chunk(200).forEach(function(tickers){
        promises.push(Q.fcall(function () {
            var query = GetBalanceSheetQuery(tickers);
            return YQL.execp(query).then(function(response) {
                return response;
            });
            
        }));
        promises.push(Q.fcall(function () {
            var query = GetMarketPriceQuery(tickers);
            return YQL.execp(query).then(function(response) {
                return response;
            });
        }));
    });

    return Q.allSettled(promises).then(function (results) {
        results.forEach(function (result) {
            if (result.state === "fulfilled") {
                var value = result.value;
                balancesheets.push(value);
            } else {
                var reason = result.reason;
                logger.debug(reason);
            }
        });
        logger.debug("Finished Reading Balancesheets");
        for (var i in balancesheets) {
            logger.debug("Processing i = " + i);
            if (!("query" in balancesheets[i])) {
                logger.warn("no query for " + JSON.stringify(balancesheets[i], null, '\t'));
                continue;
            }
            if (!("results" in balancesheets[i].query)) {
                logger.warn("no results for " + JSON.stringify(balancesheets[i].query, null, '\t'));
                continue;
            }
            if ("balancesheet" in balancesheets[i].query.results) {
                var balancesheetList = balancesheets[i].query.results.balancesheet;

                for (var j in balancesheetList) {
                    var balancesheet = balancesheetList[j];
                    var symbol = balancesheet.symbol;
                    if ("statement" in balancesheet && balancesheet.statement.length > 0) {
                        var lastStatement = balancesheet.statement[0];
                        var cash = lastStatement.CashAndCashEquivalents.content;
                        var period = lastStatement.period;
                        companyProfiles[symbol] = {};
                        companyProfiles[symbol]["CashAndCashEquivalents"] = cash;
                        companyProfiles[symbol]["CashPeriod"] = period;
                    }
                }
            } else if ("stats" in balancesheets[i].query.results) {

                var stats = balancesheets[i].query.results.stats;
                for (var j in stats){
                    var statsItem = stats[j];
                    var symbol = statsItem.symbol;
                    if (!("MarketCap" in statsItem)) {
                        logger.warn("MarketCap not in " + JSON.stringify(statsItem));
                        continue;
                    }
                    var MarketCap = statsItem.MarketCap.content;
                    if (!(symbol in companyProfiles)) companyProfiles[symbol] = {};
                    companyProfiles[symbol].MarketCap = MarketCap;

                }
            }
            logger.debug("Done processing i = " + i);
        }

        for (var symbol in companyProfiles) {
            var profile = companyProfiles[symbol];
            if (
                // parseFloat(profile.MarketCap) <= 1000000000 &&
                parseFloat(profile.MarketCap) >= 100000000 &&
                parseFloat(profile.MarketCap) * 0.65 <= parseFloat(profile.CashAndCashEquivalents)) {
                logger.debug("Found " + symbol + " marketCap < Cash");
                companyProfiles[symbol].MoreCashThanMarketCap = true;
            }
        }
        writeJSONToFile(companyProfiles, "CompanyProfiles.json");
        logger.debug("Finished Writing CompanyProfiles");
    });        
})
.then(function() {
    logger.debug("Done!");
})
.catch(function (error) {
    logger.error(error);
});

