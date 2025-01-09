import sqlite3 from 'sqlite3';
import { open } from 'sqlite';

async function initializeDatabase() {
    const db = await open({
        filename: '../resources/stocks.db',
        driver: sqlite3.Database,
    });

    // 테이블 생성 쿼리
    await db.exec(`
        CREATE TABLE IF NOT EXISTS stocks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            company_name TEXT,
            market_price REAL,
            market_cap INTEGER,
            last_updated DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    `);

    console.log('Database and table initialized.');
    return db;
}
