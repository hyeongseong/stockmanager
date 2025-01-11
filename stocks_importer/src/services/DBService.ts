import sqlite3 from 'sqlite3';
import { open, Database } from 'sqlite';
import logger from './LoggerService.js';
import * as fs from 'fs';
import * as path from 'path';

export class DBService {
    private db!: Database<sqlite3.Database, sqlite3.Statement>;

    /**
     * Initializes the database and creates the `stocks` and `assetProfile` tables if they don't exist.
     */
    public async initialize(): Promise<void> {
        const dbPath = './resources/stocks.db';
        const dbDir = path.dirname(dbPath);

        // Ensure the directory exists
        if (!fs.existsSync(dbDir)) {
            try {
                fs.mkdirSync(dbDir, { recursive: true });
                logger.info(`Created directory for database: ${dbDir}`);
            } catch (error) {
                logger.error(`Failed to create directory for database: ${dbDir}`);
                throw error;
            }
        }

        this.db = await open({
            filename: dbPath,
            driver: sqlite3.Database,
        });

        try {
            // Drop existing tables
            await this.db.exec(`DROP TABLE IF EXISTS stocks;`);
            await this.db.exec(`DROP TABLE IF EXISTS assetProfile;`);
            await this.db.exec(`DROP TABLE IF EXISTS recommendation_trend;`);
            await this.db.exec(`DROP TABLE IF EXISTS cashflow_statement_history;`);
            await this.db.exec(`DROP TABLE IF EXISTS default_key_statistics;`);
            await this.db.exec(`DROP TABLE IF EXISTS income_statement_history;`);


            // Create `stocks` table
            await this.db.exec(`
                CREATE TABLE IF NOT EXISTS stocks (
                    symbol TEXT PRIMARY KEY,
                    company_name TEXT,
                    category_id TEXT NOT NULL,
                    category_name TEXT NOT NULL,
                    market_price REAL,
                    market_cap INTEGER,
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            `);

            // Create `assetProfile` table
            await this.db.exec(`
                CREATE TABLE IF NOT EXISTS assetProfile (
                    symbol TEXT PRIMARY KEY,
                    address1 TEXT,
                    city TEXT,
                    state TEXT,
                    zip TEXT,
                    country TEXT,
                    phone TEXT,
                    website TEXT,
                    industry TEXT,
                    industryKey TEXT,
                    industryDisp TEXT,
                    sector TEXT,
                    sectorKey TEXT,
                    sectorDisp TEXT,
                    longBusinessSummary TEXT,
                    fullTimeEmployees INTEGER,
                    companyOfficers TEXT, -- JSON data
                    auditRisk INTEGER,
                    boardRisk INTEGER,
                    compensationRisk INTEGER,
                    shareHolderRightsRisk INTEGER,
                    overallRisk INTEGER,
                    governanceEpochDate INTEGER,
                    compensationAsOfEpochDate INTEGER,
                    irWebsite TEXT,
                    maxAge INTEGER,
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(symbol) REFERENCES stocks(symbol) ON DELETE CASCADE
                )
            `);

            // Create `recommendation_trend` table
            await this.db.exec(`
                CREATE TABLE IF NOT EXISTS recommendation_trend (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    period TEXT NOT NULL,
                    strongBuy INTEGER,
                    buy INTEGER,
                    hold INTEGER,
                    sell INTEGER,
                    strongSell INTEGER,
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(symbol) REFERENCES stocks(symbol) ON DELETE CASCADE
                )
            `);

            // Create `cashflow_statement_history` table
            await this.db.exec(`
                CREATE TABLE IF NOT EXISTS cashflow_statement_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    endDate_raw INTEGER,
                    endDate_fmt TEXT,
                    netIncome_raw INTEGER,
                    netIncome_fmt TEXT,
                    netIncome_longFmt TEXT,
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(symbol) REFERENCES stocks(symbol) ON DELETE CASCADE,
                    UNIQUE(symbol, endDate_raw) -- 중복 삽입 방지
                )
            `);

            // Create `index_trend` table
            await this.db.exec(`
                CREATE TABLE IF NOT EXISTS index_trend (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    period TEXT NOT NULL,
                    growth REAL,
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(symbol, period) -- 중복 삽입 방지
                )
            `);

            // Create `default_key_statistics` table
            await this.db.exec(`
                CREATE TABLE IF NOT EXISTS default_key_statistics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL UNIQUE, -- symbol에 UNIQUE 제약 조건 추가
                    priceHint INTEGER,
                    enterpriseValue INTEGER,
                    forwardPE REAL,
                    profitMargins REAL,
                    floatShares INTEGER,
                    sharesOutstanding INTEGER,
                    sharesShort INTEGER,
                    sharesShortPriorMonth INTEGER,
                    sharesShortPreviousMonthDate INTEGER,
                    dateShortInterest INTEGER,
                    sharesPercentSharesOut REAL,
                    heldPercentInsiders REAL,
                    heldPercentInstitutions REAL,
                    shortRatio REAL,
                    shortPercentOfFloat REAL,
                    beta REAL,
                    impliedSharesOutstanding INTEGER,
                    bookValue REAL,
                    priceToBook REAL,
                    lastFiscalYearEnd INTEGER,
                    nextFiscalYearEnd INTEGER,
                    mostRecentQuarter INTEGER,
                    earningsQuarterlyGrowth REAL,
                    netIncomeToCommon INTEGER,
                    trailingEps REAL,
                    forwardEps REAL,
                    lastSplitFactor TEXT,
                    lastSplitDate INTEGER,
                    enterpriseToRevenue REAL,
                    enterpriseToEbitda REAL,
                    change52Week REAL,
                    sandP52WeekChange REAL,
                    lastDividendValue REAL,
                    lastDividendDate INTEGER,
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(symbol) REFERENCES stocks(symbol) ON DELETE CASCADE
                )
            `);

            // Create `income_statement_history` table
            await this.db.exec(`
                CREATE TABLE IF NOT EXISTS income_statement_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    endDate_raw INTEGER,
                    endDate_fmt TEXT,
                    totalRevenue_raw INTEGER,
                    totalRevenue_fmt TEXT,
                    totalRevenue_longFmt TEXT,
                    costOfRevenue_raw INTEGER,
                    costOfRevenue_fmt TEXT,
                    costOfRevenue_longFmt TEXT,
                    grossProfit_raw INTEGER,
                    grossProfit_fmt TEXT,
                    grossProfit_longFmt TEXT,
                    ebit_raw INTEGER,
                    ebit_fmt TEXT,
                    ebit_longFmt TEXT,
                    incomeTaxExpense_raw INTEGER,
                    incomeTaxExpense_fmt TEXT,
                    incomeTaxExpense_longFmt TEXT,
                    netIncome_raw INTEGER,
                    netIncome_fmt TEXT,
                    netIncome_longFmt TEXT,
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(symbol) REFERENCES stocks(symbol) ON DELETE CASCADE,
                    UNIQUE(symbol, endDate_raw) -- 중복 삽입 방지
                )
            `);

            logger.info('Database initialized and tables created.');
        } catch (error) {
            logger.error('Failed to initialize database or create tables.');
            logger.error(error);
            throw error;
        }
    }

    public async upsertAssetProfile(symbol: string, assetProfile: any): Promise<void> {
        const companyOfficersJson = JSON.stringify(assetProfile.companyOfficers);

        const query = `
            INSERT INTO assetProfile (
                symbol, address1, city, state, zip, country, phone, website, industry, industryKey,
                industryDisp, sector, sectorKey, sectorDisp, longBusinessSummary, fullTimeEmployees, companyOfficers, auditRisk, boardRisk, compensationRisk,
                shareHolderRightsRisk, overallRisk, governanceEpochDate, compensationAsOfEpochDate, irWebsite, maxAge
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol) DO UPDATE SET
                address1 = excluded.address1,
                city = excluded.city,
                state = excluded.state,
                zip = excluded.zip,
                country = excluded.country,
                phone = excluded.phone,
                website = excluded.website,
                industry = excluded.industry,
                industryKey = excluded.industryKey,
                industryDisp = excluded.industryDisp,
                sector = excluded.sector,
                sectorKey = excluded.sectorKey,
                sectorDisp = excluded.sectorDisp,
                longBusinessSummary = excluded.longBusinessSummary,
                fullTimeEmployees = excluded.fullTimeEmployees,
                companyOfficers = excluded.companyOfficers,
                auditRisk = excluded.auditRisk,
                compensationRisk = excluded.compensationRisk,
                shareHolderRightsRisk = excluded.shareHolderRightsRisk,
                overallRisk = excluded.overallRisk,
                governanceEpochDate = excluded.governanceEpochDate,
                compensationAsOfEpochDate = excluded.compensationAsOfEpochDate,
                irWebsite = excluded.irWebsite,
                maxAge = excluded.maxAge,
                last_updated = CURRENT_TIMESTAMP
        `;

        const params = [
            symbol,
            assetProfile.address1,
            assetProfile.city,
            assetProfile.state,
            assetProfile.zip,
            assetProfile.country,
            assetProfile.phone,
            assetProfile.website,
            assetProfile.industry,
            assetProfile.industryKey,
            assetProfile.industryDisp,
            assetProfile.sector,
            assetProfile.sectorKey,
            assetProfile.sectorDisp,
            assetProfile.longBusinessSummary,
            assetProfile.fullTimeEmployees,
            companyOfficersJson,
            assetProfile.auditRisk,
            assetProfile.boardRisk,
            assetProfile.compensationRisk,
            assetProfile.shareHolderRightsRisk,
            assetProfile.overallRisk,
            assetProfile.governanceEpochDate,
            assetProfile.compensationAsOfEpochDate,
            assetProfile.irWebsite,
            assetProfile.maxAge
        ];

        try {
            await this.db.run(query, ...params);
        } catch (error) {
            logger.error(`Failed to upsert asset profile for symbol: ${symbol}`);
            logger.error(`Query: ${query}`);
            logger.error(`Parameters: ${JSON.stringify(params, null, 2)}`);
            if (error instanceof Error) {
                logger.error(`Error: ${error.message}`);
            } else {
                logger.error(`Unknown error: ${JSON.stringify(error)}`);
            }
            throw error;
        }
    }

    public async upsertRecommendationTrend(symbol: string, trend: any[]): Promise<void> {
        const query = `
            INSERT INTO recommendation_trend (
                symbol, period, strongBuy, buy, hold, sell, strongSell, last_updated
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        `;

        try {
            // Delete existing rows for the symbol before inserting new data
            await this.db.run("DELETE FROM recommendation_trend WHERE symbol = ?", [symbol]);

            // Insert each trend item into the table
            for (const item of trend) {
                await this.db.run(query, [
                    symbol,
                    item.period,
                    item.strongBuy,
                    item.buy,
                    item.hold,
                    item.sell,
                    item.strongSell
                ]);
            }
        } catch (error) {
            logger.error(`Failed to upsert recommendation trend for symbol: ${symbol}`);
            logger.error(error);
            throw error;
        }
    }

    public async upsertCashflowStatementHistory(symbol: string, cashflowStatements: any[]): Promise<void> {
        const query = `
            INSERT INTO cashflow_statement_history (
                symbol, endDate_raw, endDate_fmt,
                netIncome_raw, netIncome_fmt, netIncome_longFmt, last_updated
            )
            VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(symbol, endDate_raw) DO UPDATE SET
                endDate_fmt = excluded.endDate_fmt,
                netIncome_raw = excluded.netIncome_raw,
                netIncome_fmt = excluded.netIncome_fmt,
                netIncome_longFmt = excluded.netIncome_longFmt,
                last_updated = CURRENT_TIMESTAMP
        `;

        try {
            for (const statement of cashflowStatements) {
                await this.db.run(query, [
                    symbol,
                    statement.endDate?.raw || null,
                    statement.endDate?.fmt || null,

                    statement.netIncome?.raw || null,
                    statement.netIncome?.fmt || null,
                    statement.netIncome?.longFmt || null,
                ]);
            }
        } catch (error) {
            logger.error(`Failed to upsert cashflow statement history for symbol: ${symbol}`);
            logger.error(error);
            throw error;
        }
    }

    public async upsertIndexTrend(symbol: string, estimates: any[]): Promise<void> {
        const query = `
            INSERT INTO index_trend (
                symbol, period, growth, last_updated
            )
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(symbol, period) DO UPDATE SET
                growth = excluded.growth,
                last_updated = CURRENT_TIMESTAMP
        `;

        try {
            // Insert or update each estimate into the table
            for (const estimate of estimates) {
                await this.db.run(query, [
                    symbol,
                    estimate.period,
                    estimate.growth || null,
                ]);
            }
        } catch (error) {
            logger.error(`Failed to upsert index trend for symbol: ${symbol}`);
            logger.error(error);
            throw error;
        }
    }

    public async upsertDefaultKeyStatistics(symbol: string, stats: any): Promise<void> {
        const query = `
            INSERT INTO default_key_statistics (
                symbol, priceHint, enterpriseValue, forwardPE, profitMargins, floatShares, sharesOutstanding,
                sharesShort, sharesShortPriorMonth, sharesShortPreviousMonthDate, dateShortInterest, sharesPercentSharesOut,
                heldPercentInsiders, heldPercentInstitutions, shortRatio, shortPercentOfFloat, beta, impliedSharesOutstanding,
                bookValue, priceToBook, lastFiscalYearEnd, nextFiscalYearEnd, mostRecentQuarter, earningsQuarterlyGrowth,
                netIncomeToCommon, trailingEps, forwardEps, lastSplitFactor, lastSplitDate, enterpriseToRevenue,
                enterpriseToEbitda, change52Week, sandP52WeekChange, lastDividendValue, lastDividendDate, last_updated
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(symbol) DO UPDATE SET
                priceHint = excluded.priceHint,
                enterpriseValue = excluded.enterpriseValue,
                forwardPE = excluded.forwardPE,
                profitMargins = excluded.profitMargins,
                floatShares = excluded.floatShares,
                sharesOutstanding = excluded.sharesOutstanding,
                sharesShort = excluded.sharesShort,
                sharesShortPriorMonth = excluded.sharesShortPriorMonth,
                sharesShortPreviousMonthDate = excluded.sharesShortPreviousMonthDate,
                dateShortInterest = excluded.dateShortInterest,
                sharesPercentSharesOut = excluded.sharesPercentSharesOut,
                heldPercentInsiders = excluded.heldPercentInsiders,
                heldPercentInstitutions = excluded.heldPercentInstitutions,
                shortRatio = excluded.shortRatio,
                shortPercentOfFloat = excluded.shortPercentOfFloat,
                beta = excluded.beta,
                impliedSharesOutstanding = excluded.impliedSharesOutstanding,
                bookValue = excluded.bookValue,
                priceToBook = excluded.priceToBook,
                lastFiscalYearEnd = excluded.lastFiscalYearEnd,
                nextFiscalYearEnd = excluded.nextFiscalYearEnd,
                mostRecentQuarter = excluded.mostRecentQuarter,
                earningsQuarterlyGrowth = excluded.earningsQuarterlyGrowth,
                netIncomeToCommon = excluded.netIncomeToCommon,
                trailingEps = excluded.trailingEps,
                forwardEps = excluded.forwardEps,
                lastSplitFactor = excluded.lastSplitFactor,
                lastSplitDate = excluded.lastSplitDate,
                enterpriseToRevenue = excluded.enterpriseToRevenue,
                enterpriseToEbitda = excluded.enterpriseToEbitda,
                change52Week = excluded.change52Week,
                sandP52WeekChange = excluded.sandP52WeekChange,
                lastDividendValue = excluded.lastDividendValue,
                lastDividendDate = excluded.lastDividendDate,
                last_updated = CURRENT_TIMESTAMP
        `;

        const params = [
            symbol,
            stats.priceHint,
            stats.enterpriseValue,
            stats.forwardPE,
            stats.profitMargins,
            stats.floatShares,
            stats.sharesOutstanding,
            stats.sharesShort,
            stats.sharesShortPriorMonth,
            stats.sharesShortPreviousMonthDate,
            stats.dateShortInterest,
            stats.sharesPercentSharesOut,
            stats.heldPercentInsiders,
            stats.heldPercentInstitutions,
            stats.shortRatio,
            stats.shortPercentOfFloat,
            stats.beta,
            stats.impliedSharesOutstanding,
            stats.bookValue,
            stats.priceToBook,
            stats.lastFiscalYearEnd,
            stats.nextFiscalYearEnd,
            stats.mostRecentQuarter,
            stats.earningsQuarterlyGrowth,
            stats.netIncomeToCommon,
            stats.trailingEps,
            stats.forwardEps,
            stats.lastSplitFactor,
            stats.lastSplitDate,
            stats.enterpriseToRevenue,
            stats.enterpriseToEbitda,
            stats["52WeekChange"],
            stats.SandP52WeekChange,
            stats.lastDividendValue,
            stats.lastDividendDate
        ];

        try {
            await this.db.run(query, ...params);
        } catch (error) {
            logger.error(`Failed to upsert default key statistics for symbol: ${symbol}`);
            logger.error(error);
            throw error;
        }
    }

    public async upsertIncomeStatementHistory(symbol: string, statements: any[]): Promise<void> {
        const query = `
            INSERT INTO income_statement_history (
                symbol, endDate_raw, endDate_fmt,
                totalRevenue_raw, totalRevenue_fmt, totalRevenue_longFmt,
                costOfRevenue_raw, costOfRevenue_fmt, costOfRevenue_longFmt,
                grossProfit_raw, grossProfit_fmt, grossProfit_longFmt,
                ebit_raw, ebit_fmt, ebit_longFmt,
                incomeTaxExpense_raw, incomeTaxExpense_fmt, incomeTaxExpense_longFmt,
                netIncome_raw, netIncome_fmt, netIncome_longFmt, last_updated
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(symbol, endDate_raw) DO UPDATE SET
                endDate_fmt = excluded.endDate_fmt,
                totalRevenue_raw = excluded.totalRevenue_raw,
                totalRevenue_fmt = excluded.totalRevenue_fmt,
                totalRevenue_longFmt = excluded.totalRevenue_longFmt,
                costOfRevenue_raw = excluded.costOfRevenue_raw,
                costOfRevenue_fmt = excluded.costOfRevenue_fmt,
                costOfRevenue_longFmt = excluded.costOfRevenue_longFmt,
                grossProfit_raw = excluded.grossProfit_raw,
                grossProfit_fmt = excluded.grossProfit_fmt,
                grossProfit_longFmt = excluded.grossProfit_longFmt,
                ebit_raw = excluded.ebit_raw,
                ebit_fmt = excluded.ebit_fmt,
                ebit_longFmt = excluded.ebit_longFmt,
                incomeTaxExpense_raw = excluded.incomeTaxExpense_raw,
                incomeTaxExpense_fmt = excluded.incomeTaxExpense_fmt,
                incomeTaxExpense_longFmt = excluded.incomeTaxExpense_longFmt,
                netIncome_raw = excluded.netIncome_raw,
                netIncome_fmt = excluded.netIncome_fmt,
                netIncome_longFmt = excluded.netIncome_longFmt,
                last_updated = CURRENT_TIMESTAMP
        `;

        try {
            for (const statement of statements) {
                await this.db.run(query, [
                    symbol,
                    statement.endDate?.raw || null,
                    statement.endDate?.fmt || null,

                    statement.totalRevenue?.raw || null,
                    statement.totalRevenue?.fmt || null,
                    statement.totalRevenue?.longFmt || null,

                    statement.costOfRevenue?.raw || null,
                    statement.costOfRevenue?.fmt || null,
                    statement.costOfRevenue?.longFmt || null,

                    statement.grossProfit?.raw || null,
                    statement.grossProfit?.fmt || null,
                    statement.grossProfit?.longFmt || null,

                    statement.ebit?.raw || null,
                    statement.ebit?.fmt || null,
                    statement.ebit?.longFmt || null,

                    statement.incomeTaxExpense?.raw || null,
                    statement.incomeTaxExpense?.fmt || null,
                    statement.incomeTaxExpense?.longFmt || null,

                    statement.netIncome?.raw || null,
                    statement.netIncome?.fmt || null,
                    statement.netIncome?.longFmt || null,
                ]);
            }
        } catch (error) {
            logger.error(`Failed to upsert income statement history for symbol: ${symbol}`);
            logger.error(error);
            throw error;
        }
    }



    public async close(): Promise<void> {
        try {
            await this.db.close();
            logger.info('Database connection closed.');
        } catch (error) {
            logger.error('Failed to close database connection.');
            logger.error(error);
        }
    }
}
