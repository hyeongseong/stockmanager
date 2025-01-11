import sqlite3 from 'sqlite3';
import { open } from 'sqlite';
import logger from './LoggerService.js';
import * as fs from 'fs';
import * as path from 'path';
export class DBService {
    /**
     * Initializes the database and creates the `stocks` and `assetProfile` tables if they don't exist.
     */
    async initialize() {
        const dbPath = './resources/stocks.db';
        const dbDir = path.dirname(dbPath);
        // Ensure the directory exists
        if (!fs.existsSync(dbDir)) {
            try {
                fs.mkdirSync(dbDir, { recursive: true });
                logger.info(`Created directory for database: ${dbDir}`);
            }
            catch (error) {
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
            logger.info('Database initialized and tables created.');
        }
        catch (error) {
            logger.error('Failed to initialize database or create tables.');
            logger.error(error);
            throw error;
        }
    }
    async upsertAssetProfile(symbol, assetProfile) {
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
        }
        catch (error) {
            logger.error(`Failed to upsert asset profile for symbol: ${symbol}`);
            logger.error(`Query: ${query}`);
            logger.error(`Parameters: ${JSON.stringify(params, null, 2)}`);
            if (error instanceof Error) {
                logger.error(`Error: ${error.message}`);
            }
            else {
                logger.error(`Unknown error: ${JSON.stringify(error)}`);
            }
            throw error;
        }
    }
    async upsertRecommendationTrend(symbol, trend) {
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
        }
        catch (error) {
            logger.error(`Failed to upsert recommendation trend for symbol: ${symbol}`);
            logger.error(error);
            throw error;
        }
    }
    async close() {
        try {
            await this.db.close();
            logger.info('Database connection closed.');
        }
        catch (error) {
            logger.error('Failed to close database connection.');
            logger.error(error);
        }
    }
}
