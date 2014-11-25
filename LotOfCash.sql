DROP TABLE IF EXISTS LatestestBalancesheet;
CREATE TEMP TABLE LatestestBalancesheet AS
    SELECT *
    FROM balancesheet
    JOIN
        (SELECT symbol, MAX(period) AS lastestPeriod
        FROM balancesheet GROUP BY symbol) balancesheetLatestPerods
    ON
        balancesheetLatestPerods.symbol = balancesheet.symbol
        AND balancesheet.period = lastestPeriod
;
-- SELECT COUNT(*) FROM LatestestBalancesheet;

DROP TABLE IF EXISTS LatestestKeystats;
CREATE TEMP TABLE LatestestKeystats AS
    SELECT *
    FROM keystats
    JOIN
        (SELECT symbol, MAX(period) AS lastestPeriod
        FROM keystats GROUP BY symbol) keystatsLatestPerods
    ON
        keystatsLatestPerods.symbol = keystats.symbol
        AND keystats.period = lastestPeriod
;
-- SELECT COUNT(*) FROM LatestestKeystats;

DROP TABLE IF EXISTS LotOfCashCompanies;
CREATE TEMP TABLE LotOfCashCompanies AS
    SELECT * FROM (
        SELECT company.symbol AS symbol,
            company.name AS name,
            industryId,
            industry.name,
            LatestestBalancesheet.period,
            LatestestBalancesheet.CashAndCashEquivalents AS Cash,
            LatestestKeystats.MarketCap AS MCap,
            TrailingPE,
            Revenue,
            RevenuePerShare
        FROM company
        LEFT JOIN industry
        ON company.industryId = industry.id
        LEFT JOIN LatestestBalancesheet
        ON company.symbol = LatestestBalancesheet.symbol
        LEFT JOIN LatestestKeystats
        ON company.symbol = LatestestKeystats.symbol)
    WHERE
        MCap BETWEEN 100000000 AND 2000000000 -- Market Cap between 100M and 2B
        AND Cash > 0                          -- Has positive cash
        AND Cash > MCap*(0.5)                 -- Cash is greater than MCap
        AND industryId BETWEEN 800 AND 900    -- Section = Tech
        AND instr(symbol, '.') = 0;           -- Does not have "dot" in its symbol, means U.S. stock
;

-- SELECT COUNT(*) FROM LotOfCashCompanies;
-- SELECT COUNT(DISTINCT symbol) FROM LotOfCashCompanies;

SELECT * FROM LotOfCashCompanies;