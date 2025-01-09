import sqlite3 from 'sqlite3';
import { open, Database } from 'sqlite';
import logger from './LoggerService.js';
import * as fs from 'fs';
import * as path from 'path';

export class DBService {
    private db!: Database<sqlite3.Database, sqlite3.Statement>;

    /**
     * Initializes the database and creates the `stocks` table if it doesn't exist.
     */
    public async initialize(): Promise<void> {

        const dbPath = './resources/stocks.db';
        const dbDir = path.dirname(dbPath);

        // Ensure the directory exists
        if (!fs.existsSync(dbDir)) {
            fs.mkdirSync(dbDir, { recursive: true });
            logger.info(`Created directory for database: ${dbDir}`);
        }

        this.db = await open({
            filename: dbPath,
            driver: sqlite3.Database,
        });

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

        logger.info('Database initialized and table created.');
    }

    /**
     * Inserts or updates a stock entry with the given symbol, category_id, and category_name.
     */
    public async upsertCategory(
        symbol: string,
        categoryId: string,
        categoryName: string
    ): Promise<void> {
        await this.db.run(
            `
            INSERT INTO stocks (symbol, category_id, category_name, last_updated)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(symbol) DO UPDATE SET
                category_id = excluded.category_id,
                category_name = excluded.category_name,
                last_updated = CURRENT_TIMESTAMP
            `,
            symbol,
            categoryId,
            categoryName
        );

        logger.info(`Upserted stock: symbol=${symbol}, category_id=${categoryId}, category_name=${categoryName}`);
    }

    /**
     * Closes the database connection.
     */
    public async close(): Promise<void> {
        await this.db.close();
        logger.info('Database connection closed.');
    }
}
