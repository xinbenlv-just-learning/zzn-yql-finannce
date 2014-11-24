var t = {
    "query": {
        "count": 1,
        "created": "2014-11-24T18:14:37Z",
        "lang": "en-US",
        "results": {
            "stats": {
                "symbol": "T",
                "MarketCap": {
                    "term": "intraday",
                    "content": "179780000000"
                },
                "EnterpriseValue": {
                    "term": "Nov 24, 2014",
                    "content": "256160000000.00003"
                },
                "TrailingPE": {
                    "term": "ttm, intraday",
                    "content": "10.60"
                },
                "ForwardPE": {
                    "term": "fye Dec 31, 2015",
                    "content": "13.29"
                },
                "PEGRatio": {
                    "term": "5 yr expected",
                    "content": "2.98"
                },
                "PriceSales": {
                    "term": "ttm",
                    "content": "1.40"
                },
                "PriceBook": {
                    "term": "mrq",
                    "content": "1.98"
                },
                "EnterpriseValueRevenue": {
                    "term": "ttm",
                    "content": "1.95"
                },
                "EnterpriseValueEBITDA": {
                    "term": "ttm",
                    "content": "5.60"
                },
                "FiscalYearEnds": "Dec 31",
                "MostRecentQuarter": {
                    "term": "mrq",
                    "content": "Sep 30, 2014"
                },
                "ProfitMargin": {
                    "term": "ttm",
                    "content": "13.05%"
                },
                "OperatingMargin": {
                    "term": "ttm",
                    "content": "22.52%"
                },
                "ReturnonAssets": {
                    "term": "ttm",
                    "content": "6.56%"
                },
                "ReturnonEquity": {
                    "term": "ttm",
                    "content": "19.42%"
                },
                "Revenue": {
                    "term": "ttm",
                    "content": "131169999999.99998"
                },
                "RevenuePerShare": {
                    "term": "ttm",
                    "content": "25.12"
                },
                "QtrlyRevenueGrowth": {
                    "term": "yoy",
                    "content": "2.50%"
                },
                "GrossProfit": {
                    "term": "ttm",
                    "content": "77290000000"
                },
                "EBITDA": {
                    "term": "ttm",
                    "content": "45780000000"
                },
                "NetIncomeAvltoCommon": {
                    "term": "ttm",
                    "content": "17110000000"
                },
                "DilutedEPS": {
                    "term": "ttm",
                    "content": "3.27"
                },
                "QtrlyEarningsGrowth": {
                    "term": "yoy",
                    "content": "-21.30%"
                },
                "TotalCash": {
                    "term": "mrq",
                    "content": "2460000000"
                },
                "TotalCashPerShare": {
                    "term": "mrq",
                    "content": "0.47"
                },
                "TotalDebt": {
                    "term": "mrq",
                    "content": "75620000000"
                },
                "TotalDebtEquity": {
                    "term": "mrq",
                    "content": "81.21"
                },
                "CurrentRatio": {
                    "term": "mrq",
                    "content": "0.65"
                },
                "BookValuePerShare": {
                    "term": "mrq",
                    "content": "17.86"
                },
                "OperatingCashFlow": {
                    "term": "ttm",
                    "content": "33509999999.999996"
                },
                "LeveredFreeCashFlow": {
                    "term": "ttm",
                    "content": "12460000000"
                },
                "Beta": "0.19",
                "p_52_WeekChange": "-0.25%",
                "SP50052_WeekChange": "14.48%",
                "p_52_WeekHigh": {
                    "term": "Jul 29, 2014",
                    "content": "37.48"
                },
                "p_52_WeekLow": {
                    "term": "Feb 6, 2014",
                    "content": "31.74"
                },
                "p_50_DayMovingAverage": "34.76",
                "p_200_DayMovingAverage": "35.18",
                "AvgVol": [
                    {
                        "term": "3 month",
                        "content": "20,086,300"
                    },
                    {
                        "term": "10 day",
                        "content": "18,767,000"
                    }
                ],
                "SharesOutstanding": "5190000000",
                "Float": "5180000000",
                "PercentageHeldbyInsiders": "0.05%",
                "PercentageHeldbyInstitutions": "62.70%",
                "SharesShort": [
                    {
                        "term": "as of Oct 31, 2014",
                        "content": "243070000"
                    },
                    {
                        "term": "prior month",
                        "content": "225720000"
                    }
                ],
                "ShortRatio": {
                    "term": "as of Oct 31, 2014",
                    "content": "9.30"
                },
                "ShortPercentageofFloat": {
                    "term": "as of Oct 31, 2014",
                    "content": "4.70%"
                },
                "ForwardAnnualDividendRate": "1.84",
                "ForwardAnnualDividendYield": "5.20%",
                "TrailingAnnualDividendYield": [
                    "1.84",
                    "5.20%"
                ],
                "p_5YearAverageDividendYield": "5.50%",
                "PayoutRatio": "56.00%",
                "DividendDate": "Nov 3, 2014",
                "Ex_DividendDate": "Oct 8, 2014",
                "LastSplitFactor": {
                    "term": "new per old",
                    "content": "2:1"
                },
                "LastSplitDate": "Mar 20, 1998"
            }
        }
    }
}

var keys = []
for (key in t.query.results.stats) {
    if (typeof t.query.results.stats[key] === "object" && "content" in t.query.results.stats[key]) keys.push(key);
}
console.log(keys);
