(function() {

    'use strict';
    var log4js = require('log4js');
    log4js.configure({
        appenders: [{
            type: 'logLevelFilter',
            level: "INFO",
            "appender": {
                "type": "console"
            }
        },
        {
            type: 'logLevelFilter',
            level: "TRACE",
            "appender": {
                type: 'file',
                filename: (new Date()).toISOString().replace(/:/g, '-') + '.log'
            }
        }]
    });
    var logger = log4js.getLogger();
    logger.info("Start!");
    var Q = require("q");
    var util = require('util');
    var SETTINGS = {
        db: "finance.db",
        deleteDb: false,
        chunk: 100,
        usStockOnly: false,
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
            },
            {
                name: "cashflow",
                columns: [
                    "NetIncome",
                    "Depreciation",
                    "AdjustmentsToNetIncome",
                    "ChangesInAccountsReceivables",
                    "ChangesInLiabilities",
                    "ChangesInInventories",
                    "ChangesInOtherOperatingActivities",
                    "TotalCashFlowFromOperatingActivities",
                    "CapitalExpenditures",
                    "Investments",
                    "OtherCashflowsfromInvestingActivities",
                    "TotalCashFlowsFromInvestingActivities",
                    "DividendsPaid",
                    "SalePurchaseofStock",
                    "NetBorrowings",
                    "OtherCashFlowsfromFinancingActivities",
                    "TotalCashFlowsFromFinancingActivities",
                    "EffectOfExchangeRateChanges",
                    "ChangeInCashandCashEquivalents"
                ]
            },
            {
                name: "incomestatement",
                columns: [
                    "TotalRevenue",
                    "CostofRevenue",
                    "GrossProfit",
                    "ResearchDevelopment",
                    "SellingGeneralandAdministrative",
                    "NonRecurring",
                    "Others",
                    "TotalOperatingExpenses",
                    "OperatingIncomeorLoss",
                    "TotalOtherIncome_ExpensesNet",
                    "EarningsBeforeInterestAndTaxes",
                    "InterestExpense",
                    "IncomeBeforeTax",
                    "IncomeTaxExpense",
                    "MinorityInterest",
                    "NetIncomeFromContinuingOps",
                    "DiscontinuedOperations",
                    "ExtraordinaryItems",
                    "EffectOfAccountingChanges",
                    "OtherItems",
                    "NetIncome",
                    "PreferredStockAndOtherAdjustments",
                    "NetIncomeApplicableToCommonShares"

                ]
            },
            {
                name: "keystats",
                columns: [ 'MarketCap',
                    'EnterpriseValue',
                    'TrailingPE',
                    'ForwardPE',
                    'PEGRatio',
                    'PriceSales',
                    'PriceBook',
                    'EnterpriseValueRevenue',
                    'EnterpriseValueEBITDA',
                    'MostRecentQuarter',
                    'ProfitMargin',
                    'OperatingMargin',
                    'ReturnonAssets',
                    'ReturnonEquity',
                    'Revenue',
                    'RevenuePerShare',
                    'QtrlyRevenueGrowth',
                    'GrossProfit',
                    'EBITDA',
                    'NetIncomeAvltoCommon',
                    'DilutedEPS',
                    'QtrlyEarningsGrowth',
                    'TotalCash',
                    'TotalCashPerShare',
                    'TotalDebt',
                    'TotalDebtEquity',
                    'CurrentRatio',
                    'BookValuePerShare',
                    'OperatingCashFlow',
                    'LeveredFreeCashFlow',
                    'p_52_WeekHigh',
                    'p_52_WeekLow',
                    'ShortRatio',
                    'ShortPercentageofFloat',
                    'LastSplitFactor'
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
    var YQL = require('yqlp');
    var sqlite3 = require('sqlite3').verbose();
    var db = new sqlite3.Database(SETTINGS.db);
    Array.prototype.chunk = function(chunkSize) {
        var array=this;
        return [].concat.apply([],
            array.map(function(elem,i) {
                return i%chunkSize ? [] : [array.slice(i,i+chunkSize)];
            })
        );
    };
    var COUNTERS = {};
    COUNTERS["response"] = 0;

    function LoadIndustriesAndCompanies() {
        var query = 'select * from yahoo.finance.industry where id in (select industry.id from yahoo.finance.sectors)';

        logger.info("QUERY: " + query);
        return YQL.execp(query).then(function(response) {
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
            return {industries: industryToSQL, companies: companyToSQL};
        });
    }

    function DropAndCreateTables() {
        db.serialize(function() {
            SETTINGS.basicTables.forEach(function(table) {
                if (SETTINGS.deleteDb) {
                    var query = util.format("DROP TABLE IF EXISTS %s;", table.name);
                    logger.trace(query);
                    db.run(query);
                }
                var columns = "";
                table.columns.forEach(function(col){
                    columns += util.format("%s %s,", col.name, col.dataType || "string" + (col.isKey ? " PRIMARY KEY" : ""));
                });

                var query = util.format("CREATE TABLE IF NOT EXISTS %s (%s);", table.name, columns.substring(0, columns.length - 1));
                logger.trace(query);
                db.run(query, function (error) {
                    if (error) logger.error(error);
                });
            });
        });
    }

    /***
     * Save Industry and Company information to SQLite3 db
     * @param results in the form of {industries: industries, companies: companies}
     * @constructor
     */
    function SaveIndustriesAndCompanies(results) {
        logger.info("Saving Industry And Company");
        var tickers = [];
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
        logger.info("we have received " + tickers.length + " symbols");
        return tickers;
    }
    function CreateFinancialStatementsTables() {
        db.serialize(function () {
            SETTINGS.financialStatementsTables.forEach(function (table) {
                if (SETTINGS.deleteDb) {
                    var query = util.format("DROP TABLE IF EXISTS %s;", table.name)
                    logger.info(query);
                    db.run(query);
                }
                var columns = "";
                table.columns.forEach(function (col) {
                    columns += util.format("%s DOUBLE, ", col);
                });

                var query = util.format("CREATE TABLE IF NOT EXISTS %s (%s period DATE NOT NULL, %s symbol STRING NOT NULL, PRIMARY KEY (symbol, period));",
                    table.name, columns, table.name === "keystats" ? "" : "timeframe STRING NOT NULL,");
                logger.info(query);
                db.run(query, function (error) {
                    if (error) logger.error(error);
                });
            });
        });
    }

    function LoadFinancialStatements(allTickers) {
        logger.info("LoadFinancialStatements for " + allTickers.length + " tickers");
        var promises = [];

        allTickers.chunk(SETTINGS.chunk).forEach(function(tickers){
            SETTINGS.financialStatementsTables.forEach(function(table){
                promises.push(Q.fcall(function () {
                    var query = util.format("SELECT * FROM yahoo.finance.%s WHERE symbol in (%s) ", table.name, "\""
                     + tickers.join("\",\"") + "\"") + (table.name === "keystats" ? "" : " and timeframe=\"annually\"");
                    logger.trace(query);
                    return YQL.execp(query).then(function(response) {
                        logger.info(util.format("LoadFinancialStatements Promises! Progress = (%d/%d)", COUNTERS.response, promises.length));
                        COUNTERS.response = COUNTERS.response + 1;
                        return response;
                    });

                }));
            });
        });

        return Q.allSettled(promises).then(function (promiseResults) {
            logger.info("LoadFinancialStatements promises settled");
            promiseResults.forEach(function(promiseResult){
                db.serialize(function() {
                    db.run("BEGIN TRANSACTION");
                    if (promiseResult.state === "fulfilled") {
                        if ("query" in promiseResult.value && "results" in promiseResult.value.query) {
                            var results = promiseResult.value.query.results;
                            SETTINGS.financialStatementsTables.forEach(function(table) {
                                var processStatement = function (statement, timeframe, symbol) {
                                    var valueForInsert = "";
                                    table.columns.forEach(function (column) {
                                        try {
                                            if (column in statement && statement[column] && !isNaN(statement[column].content))
                                                valueForInsert += (parseFloat(statement[column].content) + ",");
                                            else {
                                                valueForInsert += "NULL,";
                                            }
                                        } catch(error) {
                                            logger.error(column);
                                            logger.error(statement);
                                            logger.error(error);
                                            logger.error("error");
                                        }
                                    });
                                    valueForInsert += "\"" + (new Date(statement.period || promiseResult.value.query.created)).toISOString() + "\",";
                                    valueForInsert += timeframe ? "\"" + timeframe + "\",": "";
                                    valueForInsert += "\"" + symbol + "\"";
                                    var query = util.format(
                                        "INSERT OR REPLACE INTO %s (%s) VALUES (%s);",
                                        table.name, table.columns.join(",") + ", period, " + (timeframe ? "timeframe,":"") + " symbol", valueForInsert);
                                    logger.trace(query);
                                    db.run(query);
                                };
                                if (table.name in results) {
                                    results[table.name].forEach(function (statementsOfCompany) {
                                        var statements = [];
                                        if ("statement" in statementsOfCompany && statementsOfCompany.statement) {
                                            if (Array.isArray(statementsOfCompany.statement)) {
                                                statements = statementsOfCompany.statement;
                                            } else {
                                                logger.trace("NOT ARRAY!" + JSON.stringify(statementsOfCompany.statement));
                                                statements.push(statementsOfCompany.statement);
                                            }
                                        } else {
                                            logger.trace("statement not exist in " + JSON.stringify(statementsOfCompany));
                                        }
                                        statements.forEach(function(statement) {
                                            processStatement(statement, statementsOfCompany.timeframe, statementsOfCompany.symbol);
                                        });
                                    });
                                } else if (table.name === "keystats" && "stats" in results) {
                                    results.stats.forEach(function (statement) {
                                        processStatement(statement, null, statement.symbol);
                                    });
                                }
                            });
                        }
                    } else {
                        logger.info("not fulfilled, " + JSON.stringify(promiseResult));
                    }
                    db.run("END");
                });
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
        if (SETTINGS.usStockOnly) {
            logger.info("U.S. Stocks Only");
            tickers = tickers.filter(function(ticker) {
                return ticker.indexOf(".") == -1;
            }).slice(0,1000);
        }
        return tickers;
    }).then(function(tickers) {
        return LoadFinancialStatements(tickers);
    }).catch(function(error) {
        logger.error(error);
    }).done(function(){
        db.close();
        logger.info("Finished!");
    });
})();
