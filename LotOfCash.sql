DROP TABLE IF EXISTS LotOfCashCompanies;
CREATE TEMP TABLE LotOfCashCompanies AS
    SELECT * FROM (
        SELECT company.symbol AS symbol,
            company.name AS name,
            industryId,
            industry.name,
            balancesheet.period,
            balancesheet.CashAndCashEquivalents AS Cash,
            keystats.MarketCap AS MCap
        FROM company
        LEFT JOIN industry
        ON company.industryId = industry.id
        LEFT JOIN balancesheet
        ON company.symbol = balancesheet.symbol
        LEFT JOIN keystats
        ON company.symbol = keystats.symbol)
    WHERE
        MCap BETWEEN 100000000 AND 2000000000 -- Market Cap between 100M and 2B
        AND Cash > 0                          -- Has positive cash
        AND Cash > MCap*(0.5)                 -- Cash is greater than MCap
        AND industryId BETWEEN 800 AND 900    -- Section = Tech
        AND instr(symbol, '.') = 0;           -- Does not have "dot" in its symbol, means U.S. stock
;

SELECT * FROM LotOfCashCompanies;