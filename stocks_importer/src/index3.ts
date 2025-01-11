import fs from 'fs';
import path from 'path';
import sqlite3 from 'sqlite3';
import { open } from 'sqlite';
import yahooFinance from 'yahoo-finance2';
yahooFinance.suppressNotices(['yahooSurvey']);


async function initializeDatabase() {

    const dbDir = path.resolve('../resources');
    // if (!fs.existsSync(dbDir)) {
    //     fs.mkdirSync(dbDir, { recursive: true });
    //     console.log('Created directory:', dbDir);
    // }

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
            assetProfile TEXT,
            recommendationTrend TEXT,
            cashflowStatementHistory TEXT,
            indexTrend TEXT,
            defaultKeyStatistics TEXT,
            industryTrend TEXT,
            quoteType TEXT,
            incomeStatementHistory TEXT,
            fundOwnership TEXT,
            summaryDetail TEXT,
            insiderHolders TEXT,
            calendarEvents TEXT,
            upgradeDowngradeHistory TEXT,
            price TEXT,
            balanceSheetHistory TEXT,
            earningsTrend TEXT,
            institutionOwnership TEXT,
            majorHoldersBreakdown TEXT,
            balanceSheetHistoryQuarterly TEXT,
            earningsHistory TEXT,
            majorDirectHolders TEXT,
            summaryProfile TEXT,
            netSharePurchaseActivity TEXT,
            insiderTransactions TEXT,
            sectorTrend TEXT,
            incomeStatementHistoryQuarterly TEXT,
            cashflowStatementHistoryQuarterly TEXT,
            earnings TEXT,
            financialData TEXT,
            last_updated DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    `);

    console.log('Database and table initialized.');
    return db;
}


async function fetchAllModules(symbol: string): Promise<void> {
    try {
        //const result = await yahooFinance.quoteSummary(symbol, { modules: "all" });
        const result = await yahooFinance.quoteSummary('AAPL', {
            modules: [
                "assetProfile",
                "recommendationTrend",
                "cashflowStatementHistory",
                "indexTrend",
                "defaultKeyStatistics",
                "industryTrend",
                "quoteType",
                "incomeStatementHistory",
                "fundOwnership",
                "summaryDetail",
                "insiderHolders",
                "calendarEvents",
                "upgradeDowngradeHistory",
                "price",
                "balanceSheetHistory",
                "earningsTrend",
                /*"secFilings",*/
                "institutionOwnership",
                "majorHoldersBreakdown",
                "balanceSheetHistoryQuarterly",
                "earningsHistory",
                "majorDirectHolders",
                "summaryProfile",
                "netSharePurchaseActivity",
                "insiderTransactions",
                "sectorTrend",
                "incomeStatementHistoryQuarterly",
                "cashflowStatementHistoryQuarterly",
                "earnings",
                "financialData",
            ]
        });

        if (result) {
            console.log(JSON.stringify(result, null, 2));
            console.log('################### Good !! #######################:');
        } else {
            console.log('No result returned.');
        }
    } catch (error) {
        console.error(error);
        console.error('################### Error !! #######################:');
    }
}

//initializeDatabase();

fetchAllModules('AAPL');
