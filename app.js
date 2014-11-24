(function() {

    'use strict';
    var log4js = require('log4js');
    var logger = log4js.getLogger();
    logger.debug("Start!");
    var Q = require("q");
    var util = require('util');
    var SETTINGS = {
        db: "finance.db",
        deleteDb: false,
        chunk: 100,
        financialStatementsTables: [
            {
                name : "balancesheet",
                columns: [
                    "CashAndCashEquivalents",
                    "ShortTermInvestments",
                    "NetReceivables",
                    "Inventory",
                    "OtherCurrentAssets",
                    "TotalCurrentAssets",
                    "LongTermInvestments",
                    "PropertyPlantandEquipment",
                    "Goodwill",
                    "IntangibleAssets",
                    "AccumulatedAmortization",
                    "OtherAssets",
                    "DeferredLongTermAssetCharges",
                    "TotalAssets",
                    "AccountsPayable",
                    "Short_CurrentLongTermDebt",
                    "OtherCurrentLiabilities",
                    "TotalCurrentLiabilities",
                    "LongTermDebt",
                    "OtherLiabilities",
                    "DeferredLongTermLiabilityCharges",
                    "MinorityInterest",
                    "NegativeGoodwill",
                    "TotalLiabilities",
                    "MiscStocksOptionsWarrants",
                    "RedeemablePreferredStock",
                    "PreferredStock",
                    "CommonStock",
                    "RetainedEarnings",
                    "TreasuryStock",
                    "CapitalSurplus",
                    "OtherStockholderEquity",
                    "TotalStockholderEquity",
                    "NetTangibleAssets"
                ]
            }

        ],
        basicTables: [{
            name: 'industry',
            columns :[{ name: 'name'}, { name: 'id', isKey: true }]
        }, {
            name: 'company',
            columns :[{ name: 'name'}, { name: 'symbol', isKey: true }, {name: 'industryId'}]
        }]
    };

    Array.prototype.chunk = function(chunkSize) {
        var array=this;
        return [].concat.apply([],
            array.map(function(elem,i) {
                return i%chunkSize ? [] : [array.slice(i,i+chunkSize)];
            })
        );
    };

    var YQL = require('yqlp');
    var sqlite3 = require('sqlite3').verbose();
    var db = new sqlite3.Database(SETTINGS.db);
    /**
     * Used as a Q promise
     */
    function LoadIndustriesAndCompanies() {
        logger.debug("loading industry");
        var query = 'select * from yahoo.finance.industry where id in (select industry.id from yahoo.finance.sectors)';
        return YQL.execp(query).then(function(response) {
            logger.debug("finished loading industry 1");
            var industryToSQL = [];
            var companyToSQL = [];
            for (var i in response.query.results.industry) {
                var industry = response.query.results.industry[i];
                industryToSQL.push({id: industry.id, name: industry.name});
                for (var c in industry.company) {
                    var company = industry.company[c];
                    companyToSQL.push({industryId: industry.id, name: company.name, symbol: company.symbol});
                }
            }
            logger.debug("finished loading industry 2");
            return {industries: industryToSQL, companies: companyToSQL};
        });
    }

    /***
     *
     * @param tables in formation of [{
     *   name: tableName,
     *   columns: [
     *     {
     *       name: columnName,
     *       dataType: dataType
     *     }
     *   ]
     * }]
     * @returns {*}
     * @constructor
     */
    function DropAndCreateTables() {
        // var db = new sqlite3.Database(SETTINGS.db);
        db.serialize(function() {
            SETTINGS.basicTables.forEach(function(table) {
                if (SETTINGS.deleteDb) {
                    var query = util.format("DROP TABLE IF EXISTS %s;", table.name);
                    logger.debug(query);
                    db.run(query);
                }
                var columns = "";
                table.columns.forEach(function(col){
                    columns += util.format("%s %s,", col.name, col.dataType || "string" + (col.isKey ? " PRIMARY KEY" : ""));
                });


                var query = util.format("CREATE TABLE IF NOT EXISTS %s (%s);", table.name, columns.substring(0, columns.length - 1));
                logger.debug(query);
                db.run(query, function (error) {
                    if (error) logger.error(error);
                });
            });
        });
        logger.debug("before close");
        // db.close();
    }

    /***
     * Save Industry and Company information to SQLite3 db
     * @param results in the form of {industries: industries, companies: companies}
     * @constructor
     */
    function SaveIndustriesAndCompanies(results) {
        logger.debug("Saving");
        var tickers = [];
        // var db = new sqlite3.Database(SETTINGS.db);
        db.serialize(function() {
            db.run("BEGIN TRANSACTION");
            results.industries.forEach(function(industry) {
                if ("id" in industry && industry.id)
                    db.run("INSERT OR REPLACE INTO industry (name, id) VALUES (?,?);", industry.name, industry.id);
            });
            results.companies.forEach(function(company) {
                if ("symbol" in company && company.symbol)
                {
                    db.run("INSERT OR REPLACE INTO company (symbol, name, industryId) VALUES (?,?,?);", company.symbol, company.name, company.industryId);
                    tickers.push(company.symbol);

                }
            });
            db.run("END");
        });
        // db.close();
        logger.debug("we have " + tickers.length + " tickers");
        return tickers;
    }
    function CreateFinancialStatementsTables() {
        // var db = new sqlite3.Database(SETTINGS.db);
        db.serialize(function() {
            SETTINGS.financialStatementsTables.forEach(function(table) {
                if (SETTINGS.deleteDb) {
                    var query = util.format("DROP TABLE IF EXISTS %s;", table.name)
                    logger.debug(query);
                    db.run(query);
                }
                var columns = "";
                table.columns.forEach(function(col) {
                    columns += util.format("%s LONG, ", col);
                });

                var query = util.format("CREATE TABLE IF NOT EXISTS %s (%s period DATE NOT NULL, timeframe STRING NOT NULL, symbol STRING NOT NULL, PRIMARY KEY (symbol, period));", table.name, columns);
                logger.debug(query);
                db.run(query, function (error) {
                    if (error) logger.error(error);
                });
            });
        });
        // db.close();
    }
    /**
     * Load BalanceSheets, IncomeStatement, CashFlow, KeyStates
     */
    var COUNTERS = {};
    COUNTERS["response"] = 0;
    function LoadFinancialStatements(allTickers) {
        logger.debug("LoadFinancialStatements for " + allTickers.length + " tickers");
        var promises = [];

        allTickers.chunk(SETTINGS.chunk).forEach(function(tickers){
            SETTINGS.financialStatementsTables.forEach(function(table){
                promises.push(Q.fcall(function () {
                    var query = util.format("SELECT * FROM yahoo.finance.%s WHERE symbol in (%s) and timeframe=\"annually\"", table.name, "\""
                     + tickers.join("\",\"") + "\"");
                    return YQL.execp(query).then(function(response) {
                        logger.debug("getting response! COUNTERS[response]=" + COUNTERS.response);
                        COUNTERS.response = COUNTERS.response + 1;
                        return response;
                    });

                }));
            });
        });

        return Q.allSettled(promises).then(function (promiseResults) {
            logger.debug("LoadFinancialStatements promises settled");
            promiseResults.forEach(function(promiseResult){
                // var db = new sqlite3.Database(SETTINGS.db);
                db.serialize(function() {
                    db.run("BEGIN TRANSACTION");
                    if (promiseResult.state === "fulfilled") {
                        if ("query" in promiseResult.value && "results" in promiseResult.value.query) {
                            var results = promiseResult.value.query.results;
                            SETTINGS.financialStatementsTables.forEach(function(table) {
                                if (table.name in results) {
                                    results[table.name].forEach(function(statementsOfCompany) {
                                        if ("statement" in statementsOfCompany && statementsOfCompany.statement) {
                                            var processStatement = function (statement) {
                                                var valueForInsert = "";
                                                table.columns.forEach(function (column) {
                                                    valueForInsert += (parseInt(statement[column].content) || "NULL") + ",";
                                                });
                                                valueForInsert += "\"" + (new Date(statement.period)).toISOString() + "\",";
                                                valueForInsert += "\"" + statementsOfCompany.timeframe + "\",";
                                                valueForInsert += "\"" + statementsOfCompany.symbol + "\"";
                                                var query = util.format(
                                                    "INSERT OR REPLACE INTO %s (%s) VALUES (%s);",
                                                    table.name, table.columns.join(",") + ",period, timeframe, symbol", valueForInsert);
                                                db.run(query);
                                            };
                                            if (Array.isArray(statementsOfCompany.statement)) {
                                                statementsOfCompany.statement.forEach(processStatement);
                                            } else {
                                                logger.warn("NOT ARRAY!" + JSON.stringify(statementsOfCompany.statement));
                                                processStatement(statementsOfCompany.statement);
                                            }
                                            logger.debug("statement exists in " + statementsOfCompany.symbol);
                                        } else {
                                            logger.warn("statement not exist in " + JSON.stringify(statementsOfCompany));
                                        }
                                    });

                                }
                            });
                        }
                    } else {
                        logger.debug("not fulfilled, " + JSON.stringify(promiseResult));
                    }
                    db.run("END");
                });
                // db.close();
            });

        });
    }

    /**
     * Run the job here
     */
    Q.fcall(function() {
        DropAndCreateTables();
        CreateFinancialStatementsTables();
    }).then(function() {
        return LoadIndustriesAndCompanies();
    }).then(function(results){
        return SaveIndustriesAndCompanies(results);
    }).then(function(tickers){
        /*tickers = tickers.filter(function(ticker) {
            return ticker.indexOf(".") == -1;
        });//.slice(0,30);*/
        logger.debug(tickers.join(","));
        return LoadFinancialStatements(tickers);

    }).catch(function(error) {
        logger.error(error);
    }).done(function(wat){
        db.close();
        logger.debug("ALL DONE!");

    });
})();
