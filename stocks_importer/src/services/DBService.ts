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
            // Drop all tables
            await this.dropTables();

            // Create `stocks` table
            await this.db.exec(`
                CREATE TABLE IF NOT EXISTS stocks (
                    symbol TEXT NOT NULL,
                    company_name TEXT,
                    category_id TEXT NOT NULL,
                    category_name TEXT NOT NULL,
                    market_price REAL,
                    market_cap INTEGER,
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (symbol)
                )
            `);

            // Create `assetProfile` table
            await this.db.exec(`
                CREATE TABLE IF NOT EXISTS assetProfile (
                    symbol TEXT NOT NULL,
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
                    companyOfficers TEXT,
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
                    PRIMARY KEY (symbol),
                    FOREIGN KEY(symbol) REFERENCES stocks(symbol) ON DELETE CASCADE
                )
            `);

            // Create `recommendation_trend` table
            await this.db.exec(`
                CREATE TABLE IF NOT EXISTS recommendation_trend (
                    symbol TEXT NOT NULL,
                    period TEXT NOT NULL,
                    strongBuy INTEGER,
                    buy INTEGER,
                    hold INTEGER,
                    sell INTEGER,
                    strongSell INTEGER,
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (symbol, period),
                    FOREIGN KEY(symbol) REFERENCES stocks(symbol) ON DELETE CASCADE
                )
            `);

            // Create `cashflow_statement_history` table
            await this.db.exec(`
                CREATE TABLE IF NOT EXISTS cashflow_statement_history (
                    symbol TEXT NOT NULL,
                    endDate_raw INTEGER NOT NULL,
                    endDate_fmt TEXT,
                    netIncome_raw INTEGER,
                    netIncome_fmt TEXT,
                    netIncome_longFmt TEXT,
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY(symbol, endDate_raw),
                    FOREIGN KEY(symbol) REFERENCES stocks(symbol) ON DELETE CASCADE
                )
            `);

            // Create `index_trend` table
            await this.db.exec(`
                CREATE TABLE IF NOT EXISTS index_trend (
                    symbol TEXT NOT NULL,
                    period TEXT NOT NULL,
                    growth REAL,
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (symbol, period),
                    FOREIGN KEY (symbol) REFERENCES stocks(symbol) ON DELETE CASCADE
                )
            `);

            // Create `default_key_statistics` table
            await this.db.exec(`
                CREATE TABLE IF NOT EXISTS default_key_statistics (
                    symbol TEXT NOT NULL,
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
                    PRIMARY KEY (symbol),
                    FOREIGN KEY(symbol) REFERENCES stocks(symbol) ON DELETE CASCADE
                )
            `);

            // Create `income_statement_history` table
            await this.db.exec(`
                CREATE TABLE IF NOT EXISTS income_statement_history (
                    symbol TEXT NOT NULL,
                    endDate_raw INTEGER NOT NULL,
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
                    PRIMARY KEY (symbol, endDate_raw),
                    FOREIGN KEY(symbol) REFERENCES stocks(symbol) ON DELETE CASCADE
                )
            `);

            // Create `fund_ownership` table
            await this.db.exec(`
                CREATE TABLE IF NOT EXISTS fund_ownership (
                    symbol TEXT NOT NULL,
                    reportDate_raw INTEGER,
                    reportDate_fmt TEXT,
                    organization TEXT,
                    pctHeld_raw REAL,
                    pctHeld_fmt TEXT,
                    position_raw INTEGER,
                    position_fmt TEXT,
                    position_longFmt TEXT,
                    value_raw INTEGER,
                    value_fmt TEXT,
                    value_longFmt TEXT,
                    pctChange_raw REAL,
                    pctChange_fmt TEXT,
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY(symbol, reportDate_raw, organization),
                    FOREIGN KEY(symbol) REFERENCES stocks(symbol) ON DELETE CASCADE
                )
            `);

            // Create `summary_detail` table
            await this.db.exec(`
                CREATE TABLE IF NOT EXISTS summary_detail (
                    symbol TEXT NOT NULL,
                    priceHint INTEGER,
                    previousClose REAL,
                    open REAL,
                    dayLow REAL,
                    dayHigh REAL,
                    regularMarketPreviousClose REAL,
                    regularMarketOpen REAL,
                    regularMarketDayLow REAL,
                    regularMarketDayHigh REAL,
                    dividendRate REAL,
                    dividendYield REAL,
                    exDividendDate INTEGER,
                    payoutRatio REAL,
                    fiveYearAvgDividendYield REAL,
                    beta REAL,
                    trailingPE REAL,
                    forwardPE REAL,
                    volume INTEGER,
                    regularMarketVolume INTEGER,
                    averageVolume INTEGER,
                    averageVolume10days INTEGER,
                    averageDailyVolume10Day INTEGER,
                    bid REAL,
                    ask REAL,
                    bidSize INTEGER,
                    askSize INTEGER,
                    marketCap INTEGER,
                    fiftyTwoWeekLow REAL,
                    fiftyTwoWeekHigh REAL,
                    priceToSalesTrailing12Months REAL,
                    fiftyDayAverage REAL,
                    twoHundredDayAverage REAL,
                    trailingAnnualDividendRate REAL,
                    trailingAnnualDividendYield REAL,
                    currency TEXT,
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY(symbol),
                    FOREIGN KEY (symbol) REFERENCES stocks(symbol) ON DELETE CASCADE
                )
            `);

            // Create `insider_holders` table
            await this.db.exec(`
                CREATE TABLE IF NOT EXISTS insider_holders (
                    symbol TEXT NOT NULL,
                    name TEXT NOT NULL,
                    relation TEXT NOT NULL,
                    url TEXT,
                    transactionDescription TEXT,
                    latestTransDate_raw INTEGER,
                    latestTransDate_fmt TEXT,
                    positionDirect_raw INTEGER,
                    positionDirect_fmt TEXT,
                    positionDirect_longFmt TEXT,
                    positionDirectDate_raw INTEGER,
                    positionDirectDate_fmt TEXT,
                    positionIndirect_raw INTEGER,
                    positionIndirect_fmt TEXT,
                    positionIndirect_longFmt TEXT,
                    positionIndirectDate_raw INTEGER,
                    positionIndirectDate_fmt TEXT,
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY(symbol, name, relation),
                    FOREIGN KEY(symbol) REFERENCES stocks(symbol) ON DELETE CASCADE
                )
            `);

            // Create `calendar_events` table
            await this.db.exec(`
                CREATE TABLE IF NOT EXISTS calendar_events (
                    symbol TEXT NOT NULL,
                    earningsDate_raw INTEGER NOT NULL,
                    earningsCallDate_raw INTEGER,
                    isEarningsDateEstimate INTEGER,
                    earningsAverage REAL,
                    earningsLow REAL,
                    earningsHigh REAL,
                    revenueAverage INTEGER,
                    revenueLow INTEGER,
                    revenueHigh INTEGER,
                    exDividendDate INTEGER,
                    dividendDate INTEGER,
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (symbol, earningsDate_raw),
                    FOREIGN KEY (symbol) REFERENCES stocks(symbol) ON DELETE CASCADE
                );
            `);

            // Create `upgrade_downgrade_history` table
            await this.db.exec(`
                CREATE TABLE IF NOT EXISTS upgrade_downgrade_history (
                    symbol TEXT NOT NULL,
                    epochGradeDate INTEGER NOT NULL,
                    firm TEXT NOT NULL,
                    toGrade TEXT,
                    fromGrade TEXT,
                    action TEXT NOT NULL,
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY(symbol, epochGradeDate, firm),
                    FOREIGN KEY(symbol) REFERENCES stocks(symbol) ON DELETE CASCADE
                )
            `);

            // Create `price` table
            await this.db.exec(`
                CREATE TABLE IF NOT EXISTS price (
                    symbol TEXT NOT NULL,
                    preMarketSource TEXT,
                    postMarketChangePercent REAL,
                    postMarketChange REAL,
                    postMarketTime INTEGER,
                    postMarketPrice REAL,
                    postMarketSource TEXT,
                    regularMarketChangePercent REAL,
                    regularMarketChange REAL,
                    regularMarketTime INTEGER,
                    priceHint INTEGER,
                    regularMarketPrice REAL,
                    regularMarketDayHigh REAL,
                    regularMarketDayLow REAL,
                    regularMarketVolume INTEGER,
                    averageDailyVolume10Day INTEGER,
                    averageDailyVolume3Month INTEGER,
                    regularMarketPreviousClose REAL,
                    regularMarketSource TEXT,
                    regularMarketOpen REAL,
                    exchange TEXT,
                    exchangeName TEXT,
                    exchangeDataDelayedBy INTEGER,
                    marketState TEXT,
                    quoteType TEXT,
                    shortName TEXT,
                    longName TEXT,
                    currency TEXT,
                    quoteSourceName TEXT,
                    currencySymbol TEXT,
                    fromCurrency TEXT,
                    toCurrency TEXT,
                    lastMarket TEXT,
                    marketCap INTEGER,
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY(symbol),
                    FOREIGN KEY(symbol) REFERENCES stocks(symbol) ON DELETE CASCADE
                )
            `);

            // Create `balance_sheet_history` table
            await this.db.exec(`
                CREATE TABLE IF NOT EXISTS balance_sheet_history (
                    symbol TEXT NOT NULL,
                    endDate_raw INTEGER NOT NULL,
                    endDate_fmt TEXT,
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY(symbol, endDate_raw),
                    FOREIGN KEY(symbol) REFERENCES stocks(symbol) ON DELETE CASCADE
                )
            `);

            // Create `earnings_trend` table
            await this.db.exec(`
                CREATE TABLE IF NOT EXISTS earnings_trend (
                    symbol TEXT NOT NULL,
                    period TEXT NOT NULL,
                    endDate TEXT,
                    growth REAL,
                    earnings_avg REAL,
                    earnings_low REAL,
                    earnings_high REAL,
                    yearAgoEps REAL,
                    earnings_analysts INTEGER,
                    earnings_growth REAL,
                    revenue_avg INTEGER,
                    revenue_low INTEGER,
                    revenue_high INTEGER,
                    yearAgoRevenue INTEGER,
                    revenue_analysts INTEGER,
                    revenue_growth REAL,
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (symbol, period),
                    FOREIGN KEY(symbol) REFERENCES stocks(symbol) ON DELETE CASCADE
                )
            `);

            // Create `institution_ownership` table
            await this.db.exec(`
                CREATE TABLE IF NOT EXISTS institution_ownership (
                    symbol TEXT NOT NULL,
                    reportDate_raw INTEGER,
                    reportDate_fmt TEXT,
                    organization TEXT NOT NULL,
                    pctHeld_raw REAL,
                    pctHeld_fmt TEXT,
                    position_raw INTEGER,
                    position_fmt TEXT,
                    position_longFmt TEXT,
                    value_raw INTEGER,
                    value_fmt TEXT,
                    value_longFmt TEXT,
                    pctChange_raw REAL,
                    pctChange_fmt TEXT,
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY(symbol, reportDate_raw, organization),
                    FOREIGN KEY(symbol) REFERENCES stocks(symbol) ON DELETE CASCADE
                )
            `);

            // Create `major_holders_breakdown` table
            await this.db.exec(`
                CREATE TABLE IF NOT EXISTS major_holders_breakdown (
                    symbol TEXT NOT NULL,
                    insidersPercentHeld REAL,
                    institutionsPercentHeld REAL,
                    institutionsFloatPercentHeld REAL,
                    institutionsCount INTEGER,
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (symbol),
                    FOREIGN KEY(symbol) REFERENCES stocks(symbol) ON DELETE CASCADE
                )
            `);

            // Create `balance_sheet_history_quarterly` table
            await this.db.exec(`
                CREATE TABLE IF NOT EXISTS balance_sheet_history_quarterly (
                    symbol TEXT NOT NULL,
                    endDate_raw INTEGER NOT NULL,
                    endDate_fmt TEXT NOT NULL,
                    maxAge INTEGER,
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (symbol, endDate_raw),
                    FOREIGN KEY(symbol) REFERENCES stocks(symbol) ON DELETE CASCADE
                )
            `);

            // Create `earnings_history` table
            await this.db.exec(`
                CREATE TABLE IF NOT EXISTS earnings_history (
                    symbol TEXT NOT NULL,
                    quarter_raw INTEGER NOT NULL,
                    quarter_fmt TEXT NOT NULL,
                    epsActual_raw REAL,
                    epsActual_fmt TEXT,
                    epsEstimate_raw REAL,
                    epsEstimate_fmt TEXT,
                    epsDifference_raw REAL,
                    epsDifference_fmt TEXT,
                    surprisePercent_raw REAL,
                    surprisePercent_fmt TEXT,
                    currency TEXT,
                    period TEXT,
                    maxAge INTEGER,
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY(symbol, quarter_raw),
                    FOREIGN KEY(symbol) REFERENCES stocks(symbol) ON DELETE CASCADE
                )
            `);

            // Create `major_direct_holders` table
            await this.db.exec(`
                CREATE TABLE IF NOT EXISTS major_direct_holders (
                    symbol TEXT NOT NULL,
                    maxAge INTEGER,
                    holders TEXT,
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY(symbol),
                    FOREIGN KEY(symbol) REFERENCES stocks(symbol) ON DELETE CASCADE
                )
            `);

            // Create `summary_profile` table
            await this.db.exec(`
                CREATE TABLE IF NOT EXISTS summary_profile (
                    symbol TEXT NOT NULL,
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
                    companyOfficers TEXT,
                    irWebsite TEXT,
                    maxAge INTEGER,
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY(symbol),
                    FOREIGN KEY(symbol) REFERENCES stocks(symbol) ON DELETE CASCADE
                )
            `);

            // Create `net_share_purchase_activity` table
            await this.db.exec(`
                CREATE TABLE IF NOT EXISTS net_share_purchase_activity (
                    symbol TEXT NOT NULL,
                    maxAge INTEGER,
                    period TEXT,
                    buyInfoCount INTEGER,
                    buyInfoShares INTEGER,
                    buyPercentInsiderShares REAL,
                    sellInfoCount INTEGER,
                    sellInfoShares INTEGER,
                    sellPercentInsiderShares REAL,
                    netInfoCount INTEGER,
                    netInfoShares INTEGER,
                    netPercentInsiderShares REAL,
                    totalInsiderShares INTEGER,
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY(symbol),
                    FOREIGN KEY(symbol) REFERENCES stocks(symbol) ON DELETE CASCADE
                )
            `);

            // Create `insider_transactions` table
            await this.db.exec(`
                CREATE TABLE IF NOT EXISTS insider_transactions (
                    symbol TEXT NOT NULL,
                    filerName TEXT NOT NULL,
                    startDate INTEGER NOT NULL,
                    shares_raw INTEGER NOT NULL,
                    shares_fmt TEXT,
                    shares_longFmt TEXT,
                    value_raw INTEGER,
                    value_fmt TEXT,
                    value_longFmt TEXT,
                    filerRelation TEXT,
                    transactionText TEXT,
                    ownership TEXT,
                    maxAge INTEGER,
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY(symbol, filerName, startDate),
                    FOREIGN KEY(symbol) REFERENCES stocks(symbol) ON DELETE CASCADE
                );
            `);

            // Create `sector_trend` table
            await this.db.exec(`
                CREATE TABLE IF NOT EXISTS sector_trend (
                    symbol TEXT NOT NULL,
                    maxAge INTEGER,
                    estimate TEXT,
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY(symbol),
                    FOREIGN KEY(symbol) REFERENCES stocks(symbol) ON DELETE CASCADE
                )
            `);

            // Create `income_statement_history_quarterly` table
            await this.db.exec(`
                CREATE TABLE IF NOT EXISTS income_statement_history_quarterly (
                    symbol TEXT NOT NULL,
                    endDate_raw INTEGER NOT NULL,
                    totalRevenue_raw INTEGER,
                    totalRevenue_fmt TEXT,
                    totalRevenue_longFmt TEXT,
                    netIncome_raw INTEGER,
                    netIncome_fmt TEXT,
                    netIncome_longFmt TEXT,
                    PRIMARY KEY (symbol, endDate_raw),
                    FOREIGN KEY(symbol) REFERENCES stocks(symbol) ON DELETE CASCADE
                );
            `);

            // Create `cashflow_statement_history_quarterly` table
            await this.db.exec(`
                CREATE TABLE IF NOT EXISTS cashflow_statement_history_quarterly (
                    symbol TEXT NOT NULL,
                    end_date INTEGER NOT NULL,
                    net_income_raw INTEGER,
                    net_income_fmt TEXT,
                    net_income_long_fmt TEXT,
                    PRIMARY KEY (symbol, end_date),
                    FOREIGN KEY(symbol) REFERENCES stocks(symbol) ON DELETE CASCADE
                );
            `);

            // Create tables for earnings part
            await this.db.exec(`
                CREATE TABLE IF NOT EXISTS earnings_chart (
                    symbol TEXT NOT NULL,
                    date TEXT NOT NULL,
                    actual REAL,
                    estimate REAL,
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (symbol, date),
                    FOREIGN KEY(symbol) REFERENCES stocks(symbol) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS current_quarter_estimate (
                    symbol TEXT NOT NULL PRIMARY KEY,
                    current_estimate REAL,
                    estimate_date TEXT,
                    estimate_year INTEGER,
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(symbol) REFERENCES stocks(symbol) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS earnings_date (
                    symbol TEXT NOT NULL,
                    earnings_date INTEGER,
                    is_estimate INTEGER,
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY(symbol),
                    FOREIGN KEY(symbol) REFERENCES stocks(symbol) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS financial_chart_yearly (
                    symbol TEXT NOT NULL,
                    date TEXT,
                    revenue INTEGER,
                    earnings INTEGER,
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (symbol, date),
                    FOREIGN KEY(symbol) REFERENCES stocks(symbol) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS financial_chart_quarterly (
                    symbol TEXT NOT NULL,
                    date TEXT,
                    revenue INTEGER,
                    earnings INTEGER,
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (symbol, date),
                    FOREIGN KEY(symbol) REFERENCES stocks(symbol) ON DELETE CASCADE
                );
            `);

            // Create `financialData` table
            await this.db.exec(`
                CREATE TABLE IF NOT EXISTS financial_data (
                    symbol TEXT NOT NULL,
                    current_price REAL,
                    target_high_price REAL,
                    target_low_price REAL,
                    target_mean_price REAL,
                    target_median_price REAL,
                    recommendation_mean REAL,
                    recommendation_key TEXT,
                    number_of_analyst_opinions INTEGER,
                    total_cash INTEGER,
                    total_cash_per_share REAL,
                    ebitda INTEGER,
                    total_debt INTEGER,
                    quick_ratio REAL,
                    current_ratio REAL,
                    total_revenue INTEGER,
                    debt_to_equity REAL,
                    revenue_per_share REAL,
                    return_on_assets REAL,
                    return_on_equity REAL,
                    gross_profits INTEGER,
                    free_cashflow INTEGER,
                    operating_cashflow INTEGER,
                    earnings_growth REAL,
                    revenue_growth REAL,
                    gross_margins REAL,
                    ebitda_margins REAL,
                    operating_margins REAL,
                    profit_margins REAL,
                    financial_currency TEXT,
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY(symbol),
                    FOREIGN KEY(symbol) REFERENCES stocks(symbol) ON DELETE CASCADE
                );
            `);

            logger.info('Database initialized and tables created.');
        } catch (error) {
            logger.error('Failed to initialize database or create tables.');
            logger.error(error);
            throw error;
        }
    }

    /**
     * Drops all tables from the database.
     */
    public async dropTables(): Promise<void> {
        try {
            const tables = [
                'stocks',
                'assetProfile',
                'recommendation_trend',
                'cashflow_statement_history',
                'default_key_statistics',
                'income_statement_history',
                'fund_ownership',
                'summary_detail',
                'insider_holders',
                'calendar_events',
                'upgrade_downgrade_history',
                'price',
                'balance_sheet_history',
                'earnings_trend',
                'institution_ownership',
                'major_holders_breakdown',
                'balance_sheet_history_quarterly',
                'earnings_history',
                'major_direct_holders',
                'summary_profile',
                'net_share_purchase_activity',
                'insider_transactions',
                'sector_trend',
                'income_statement_history_quarterly',
                'cashflow_statement_history_quarterly',
                'earnings_chart',
                'current_quarter_estimate',
                'earnings_date',
                'financial_chart_yearly',
                'financial_chart_quarterly',
                'financial_data'
            ];

            for (const table of tables) {
                await this.db.exec(`DROP TABLE IF EXISTS ${table};`);
            }

            logger.info('All tables have been dropped successfully.');
        } catch (error) {
            logger.error('Failed to drop tables.');
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
            ON CONFLICT(symbol, period) DO UPDATE SET
                strongBuy = excluded.strongBuy,
                buy = excluded.buy,
                hold = excluded.hold,
                sell = excluded.sell,
                strongSell = excluded.strongSell,
                last_updated = CURRENT_TIMESTAMP
        `;

        try {
            // Insert or update each trend item
            for (const item of trend) {
                await this.db.run(query, [
                    symbol,
                    item.period,
                    item.strongBuy || null,
                    item.buy || null,
                    item.hold || null,
                    item.sell || null,
                    item.strongSell || null,
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
                const params = [
                    symbol,
                    statement.endDate?.raw || null,
                    statement.endDate?.fmt || null,
                    statement.netIncome?.raw || null,
                    statement.netIncome?.fmt || null,
                    statement.netIncome?.longFmt || null,
                ];
                await this.db.run(query, params);
            }
        } catch (error) {
            logger.error(`Failed to upsert cashflow statement history for symbol: ${symbol}`);
            logger.error(`Error: ${error instanceof Error ? error.message : JSON.stringify(error)}`);
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
                const params = [
                    symbol,
                    estimate.period,
                    estimate.growth || null,
                ];
                await this.db.run(query, params);
            }
        } catch (error) {
            logger.error(`Failed to upsert index trend for symbol: ${symbol}`);
            logger.error(error instanceof Error ? error.message : JSON.stringify(error));
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
            stats.priceHint || null,
            stats.enterpriseValue || null,
            stats.forwardPE || null,
            stats.profitMargins || null,
            stats.floatShares || null,
            stats.sharesOutstanding || null,
            stats.sharesShort || null,
            stats.sharesShortPriorMonth || null,
            stats.sharesShortPreviousMonthDate || null,
            stats.dateShortInterest || null,
            stats.sharesPercentSharesOut || null,
            stats.heldPercentInsiders || null,
            stats.heldPercentInstitutions || null,
            stats.shortRatio || null,
            stats.shortPercentOfFloat || null,
            stats.beta || null,
            stats.impliedSharesOutstanding || null,
            stats.bookValue || null,
            stats.priceToBook || null,
            stats.lastFiscalYearEnd || null,
            stats.nextFiscalYearEnd || null,
            stats.mostRecentQuarter || null,
            stats.earningsQuarterlyGrowth || null,
            stats.netIncomeToCommon || null,
            stats.trailingEps || null,
            stats.forwardEps || null,
            stats.lastSplitFactor || null,
            stats.lastSplitDate || null,
            stats.enterpriseToRevenue || null,
            stats.enterpriseToEbitda || null,
            stats["52WeekChange"] || null,
            stats.SandP52WeekChange || null,
            stats.lastDividendValue || null,
            stats.lastDividendDate || null,
        ];

        try {
            await this.db.run(query, ...params);
        } catch (error) {
            logger.error(`Failed to upsert default key statistics for symbol: ${symbol}`);
            logger.error(error instanceof Error ? error.message : error);
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
            logger.error(error instanceof Error ? error.message : JSON.stringify(error));
            throw error;
        }
    }

    public async upsertFundOwnership(symbol: string, ownershipList: any[]): Promise<void> {
        const query = `
            INSERT INTO fund_ownership (
                symbol, reportDate_raw, reportDate_fmt, organization,
                pctHeld_raw, pctHeld_fmt,
                position_raw, position_fmt, position_longFmt,
                value_raw, value_fmt, value_longFmt,
                pctChange_raw, pctChange_fmt, last_updated
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(symbol, reportDate_raw, organization) DO UPDATE SET
                reportDate_fmt = excluded.reportDate_fmt,
                pctHeld_raw = excluded.pctHeld_raw,
                pctHeld_fmt = excluded.pctHeld_fmt,
                position_raw = excluded.position_raw,
                position_fmt = excluded.position_fmt,
                position_longFmt = excluded.position_longFmt,
                value_raw = excluded.value_raw,
                value_fmt = excluded.value_fmt,
                value_longFmt = excluded.value_longFmt,
                pctChange_raw = excluded.pctChange_raw,
                pctChange_fmt = excluded.pctChange_fmt,
                last_updated = CURRENT_TIMESTAMP
        `;

        try {
            for (const ownership of ownershipList) {
                const params = [
                    symbol,
                    ownership.reportDate?.raw || null,
                    ownership.reportDate?.fmt || null,
                    ownership.organization || null,

                    ownership.pctHeld?.raw || null,
                    ownership.pctHeld?.fmt || null,

                    ownership.position?.raw || null,
                    ownership.position?.fmt || null,
                    ownership.position?.longFmt || null,

                    ownership.value?.raw || null,
                    ownership.value?.fmt || null,
                    ownership.value?.longFmt || null,

                    ownership.pctChange?.raw || null,
                    ownership.pctChange?.fmt || null,
                ];

                await this.db.run(query, params);
            }
        } catch (error) {
            logger.error(`Failed to upsert fund ownership data for symbol: ${symbol}`);
            logger.error(error instanceof Error ? error.message : JSON.stringify(error));
            throw error;
        }
    }

    public async upsertSummaryDetail(symbol: string, summaryDetail: any): Promise<void> {
        const query = `
            INSERT INTO summary_detail (
                symbol,
                priceHint,
                previousClose,
                open,
                dayLow,
                dayHigh,
                regularMarketPreviousClose,
                regularMarketOpen,
                regularMarketDayLow,
                regularMarketDayHigh,
                dividendRate,
                dividendYield,
                exDividendDate,
                payoutRatio,
                fiveYearAvgDividendYield,
                beta,
                trailingPE,
                forwardPE,
                volume,
                regularMarketVolume,
                averageVolume,
                averageVolume10days,
                averageDailyVolume10Day,
                bid,
                ask,
                bidSize,
                askSize,
                marketCap,
                fiftyTwoWeekLow,
                fiftyTwoWeekHigh,
                priceToSalesTrailing12Months,
                fiftyDayAverage,
                twoHundredDayAverage,
                trailingAnnualDividendRate,
                trailingAnnualDividendYield,
                currency
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol) DO UPDATE SET
                priceHint = excluded.priceHint,
                previousClose = excluded.previousClose,
                open = excluded.open,
                dayLow = excluded.dayLow,
                dayHigh = excluded.dayHigh,
                regularMarketPreviousClose = excluded.regularMarketPreviousClose,
                regularMarketOpen = excluded.regularMarketOpen,
                regularMarketDayLow = excluded.regularMarketDayLow,
                regularMarketDayHigh = excluded.regularMarketDayHigh,
                dividendRate = excluded.dividendRate,
                dividendYield = excluded.dividendYield,
                exDividendDate = excluded.exDividendDate,
                payoutRatio = excluded.payoutRatio,
                fiveYearAvgDividendYield = excluded.fiveYearAvgDividendYield,
                beta = excluded.beta,
                trailingPE = excluded.trailingPE,
                forwardPE = excluded.forwardPE,
                volume = excluded.volume,
                regularMarketVolume = excluded.regularMarketVolume,
                averageVolume = excluded.averageVolume,
                averageVolume10days = excluded.averageVolume10days,
                averageDailyVolume10Day = excluded.averageDailyVolume10Day,
                bid = excluded.bid,
                ask = excluded.ask,
                bidSize = excluded.bidSize,
                askSize = excluded.askSize,
                marketCap = excluded.marketCap,
                fiftyTwoWeekLow = excluded.fiftyTwoWeekLow,
                fiftyTwoWeekHigh = excluded.fiftyTwoWeekHigh,
                priceToSalesTrailing12Months = excluded.priceToSalesTrailing12Months,
                fiftyDayAverage = excluded.fiftyDayAverage,
                twoHundredDayAverage = excluded.twoHundredDayAverage,
                trailingAnnualDividendRate = excluded.trailingAnnualDividendRate,
                trailingAnnualDividendYield = excluded.trailingAnnualDividendYield,
                currency = excluded.currency,
                last_updated = CURRENT_TIMESTAMP
        `;

        const params = [
            symbol,
            summaryDetail.priceHint || null,
            summaryDetail.previousClose || null,
            summaryDetail.open || null,
            summaryDetail.dayLow || null,
            summaryDetail.dayHigh || null,
            summaryDetail.regularMarketPreviousClose || null,
            summaryDetail.regularMarketOpen || null,
            summaryDetail.regularMarketDayLow || null,
            summaryDetail.regularMarketDayHigh || null,
            summaryDetail.dividendRate || null,
            summaryDetail.dividendYield || null,
            summaryDetail.exDividendDate || null,
            summaryDetail.payoutRatio || null,
            summaryDetail.fiveYearAvgDividendYield || null,
            summaryDetail.beta || null,
            summaryDetail.trailingPE || null,
            summaryDetail.forwardPE || null,
            summaryDetail.volume || null,
            summaryDetail.regularMarketVolume || null,
            summaryDetail.averageVolume || null,
            summaryDetail.averageVolume10days || null,
            summaryDetail.averageDailyVolume10Day || null,
            summaryDetail.bid || null,
            summaryDetail.ask || null,
            summaryDetail.bidSize || null,
            summaryDetail.askSize || null,
            summaryDetail.marketCap || null,
            summaryDetail.fiftyTwoWeekLow || null,
            summaryDetail.fiftyTwoWeekHigh || null,
            summaryDetail.priceToSalesTrailing12Months || null,
            summaryDetail.fiftyDayAverage || null,
            summaryDetail.twoHundredDayAverage || null,
            summaryDetail.trailingAnnualDividendRate || null,
            summaryDetail.trailingAnnualDividendYield || null,
            summaryDetail.currency || null,
        ];

        try {
            await this.db.run(query, ...params);
        } catch (error) {
            logger.error(`Failed to upsert summary detail for symbol: ${symbol}`);
            logger.error(`Error: ${error instanceof Error ? error.message : JSON.stringify(error)}`);
            throw error;
        }
    }

    public async upsertInsiderHolders(symbol: string, insiderHolders: any[]): Promise<void> {
        const query = `
            INSERT INTO insider_holders (
                symbol, name, relation, url, transactionDescription,
                latestTransDate_raw, latestTransDate_fmt,
                positionDirect_raw, positionDirect_fmt, positionDirect_longFmt,
                positionDirectDate_raw, positionDirectDate_fmt,
                positionIndirect_raw, positionIndirect_fmt, positionIndirect_longFmt,
                positionIndirectDate_raw, positionIndirectDate_fmt,
                last_updated
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(symbol, name, relation) DO UPDATE SET
                url = excluded.url,
                transactionDescription = excluded.transactionDescription,
                latestTransDate_raw = excluded.latestTransDate_raw,
                latestTransDate_fmt = excluded.latestTransDate_fmt,
                positionDirect_raw = excluded.positionDirect_raw,
                positionDirect_fmt = excluded.positionDirect_fmt,
                positionDirect_longFmt = excluded.positionDirect_longFmt,
                positionDirectDate_raw = excluded.positionDirectDate_raw,
                positionDirectDate_fmt = excluded.positionDirectDate_fmt,
                positionIndirect_raw = excluded.positionIndirect_raw,
                positionIndirect_fmt = excluded.positionIndirect_fmt,
                positionIndirect_longFmt = excluded.positionIndirect_longFmt,
                positionIndirectDate_raw = excluded.positionIndirectDate_raw,
                positionIndirectDate_fmt = excluded.positionIndirectDate_fmt,
                last_updated = CURRENT_TIMESTAMP
        `;

        try {
            for (const holder of insiderHolders) {
                await this.db.run(query, [
                    symbol,
                    holder.name || null,
                    holder.relation || null,
                    holder.url || null,
                    holder.transactionDescription || null,
                    holder.latestTransDate?.raw || null,
                    holder.latestTransDate?.fmt || null,
                    holder.positionDirect?.raw || null,
                    holder.positionDirect?.fmt || null,
                    holder.positionDirect?.longFmt || null,
                    holder.positionDirectDate?.raw || null,
                    holder.positionDirectDate?.fmt || null,
                    holder.positionIndirect?.raw || null,
                    holder.positionIndirect?.fmt || null,
                    holder.positionIndirect?.longFmt || null,
                    holder.positionIndirectDate?.raw || null,
                    holder.positionIndirectDate?.fmt || null,
                ]);
            }
        } catch (error) {
            logger.error(`Failed to upsert insider holders for symbol: ${symbol}`);
            logger.error(`Error: ${error instanceof Error ? error.message : JSON.stringify(error)}`);
            throw error;
        }
    }

    public async upsertCalendarEvents(symbol: string, calendarEvents: any): Promise<void> {
        const earnings = calendarEvents.earnings || {};
        const query = `
            INSERT INTO calendar_events (
                symbol,
                earningsDate_raw,
                earningsCallDate_raw,
                isEarningsDateEstimate,
                earningsAverage,
                earningsLow,
                earningsHigh,
                revenueAverage,
                revenueLow,
                revenueHigh,
                exDividendDate,
                dividendDate,
                last_updated
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(symbol, earningsDate_raw) DO UPDATE SET
                earningsCallDate_raw = excluded.earningsCallDate_raw,
                isEarningsDateEstimate = excluded.isEarningsDateEstimate,
                earningsAverage = excluded.earningsAverage,
                earningsLow = excluded.earningsLow,
                earningsHigh = excluded.earningsHigh,
                revenueAverage = excluded.revenueAverage,
                revenueLow = excluded.revenueLow,
                revenueHigh = excluded.revenueHigh,
                exDividendDate = excluded.exDividendDate,
                dividendDate = excluded.dividendDate,
                last_updated = CURRENT_TIMESTAMP
        `;

        const params = [
            symbol,
            earnings.earningsDate?.[0] || null,
            earnings.earningsCallDate?.[0] || null,
            earnings.isEarningsDateEstimate ? 1 : 0,
            earnings.earningsAverage || null,
            earnings.earningsLow || null,
            earnings.earningsHigh || null,
            earnings.revenueAverage || null,
            earnings.revenueLow || null,
            earnings.revenueHigh || null,
            calendarEvents.exDividendDate || null,
            calendarEvents.dividendDate || null,
        ];

        try {
            await this.db.run(query, ...params);
        } catch (error) {
            logger.error(`Failed to upsert calendar events for symbol: ${symbol}`);
            logger.error(`Query: ${query}`);
            logger.error(`Parameters: ${JSON.stringify(params, null, 2)}`);
            logger.error(`Error: ${error instanceof Error ? error.message : JSON.stringify(error)}`);
            throw error;
        }
    }

    public async upsertUpgradeDowngradeHistory(symbol: string, history: any[]): Promise<void> {
        const query = `
            INSERT INTO upgrade_downgrade_history (
                symbol, epochGradeDate, firm, toGrade, fromGrade, action, last_updated
            )
            VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(symbol, epochGradeDate, firm) DO UPDATE SET
                toGrade = excluded.toGrade,
                fromGrade = excluded.fromGrade,
                action = excluded.action,
                last_updated = CURRENT_TIMESTAMP
        `;

        try {
            for (const record of history) {
                const params = [
                    symbol,
                    record.epochGradeDate || null,
                    record.firm || null,
                    record.toGrade || null,
                    record.fromGrade || null,
                    record.action || null
                ];

                await this.db.run(query, params);
            }
        } catch (error) {
            logger.error(`Failed to upsert upgrade/downgrade history for symbol: ${symbol}`);
            logger.error(`Error: ${error instanceof Error ? error.message : JSON.stringify(error)}`);
            throw error;
        }
    }

    public async upsertPrice(symbol: string, priceData: any): Promise<void> {
        const query = `
            INSERT INTO price (
                symbol,
                preMarketSource,
                postMarketChangePercent,
                postMarketChange,
                postMarketTime,
                postMarketPrice,
                postMarketSource,
                regularMarketChangePercent,
                regularMarketChange,
                regularMarketTime,
                priceHint,
                regularMarketPrice,
                regularMarketDayHigh,
                regularMarketDayLow,
                regularMarketVolume,
                averageDailyVolume10Day,
                averageDailyVolume3Month,
                regularMarketPreviousClose,
                regularMarketSource,
                regularMarketOpen,
                exchange,
                exchangeName,
                exchangeDataDelayedBy,
                marketState,
                quoteType,
                shortName,
                longName,
                currency,
                quoteSourceName,
                currencySymbol,
                fromCurrency,
                toCurrency,
                lastMarket,
                marketCap,
                last_updated
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(symbol) DO UPDATE SET
                preMarketSource = excluded.preMarketSource,
                postMarketChangePercent = excluded.postMarketChangePercent,
                postMarketChange = excluded.postMarketChange,
                postMarketTime = excluded.postMarketTime,
                postMarketPrice = excluded.postMarketPrice,
                postMarketSource = excluded.postMarketSource,
                regularMarketChangePercent = excluded.regularMarketChangePercent,
                regularMarketChange = excluded.regularMarketChange,
                regularMarketTime = excluded.regularMarketTime,
                priceHint = excluded.priceHint,
                regularMarketPrice = excluded.regularMarketPrice,
                regularMarketDayHigh = excluded.regularMarketDayHigh,
                regularMarketDayLow = excluded.regularMarketDayLow,
                regularMarketVolume = excluded.regularMarketVolume,
                averageDailyVolume10Day = excluded.averageDailyVolume10Day,
                averageDailyVolume3Month = excluded.averageDailyVolume3Month,
                regularMarketPreviousClose = excluded.regularMarketPreviousClose,
                regularMarketSource = excluded.regularMarketSource,
                regularMarketOpen = excluded.regularMarketOpen,
                exchange = excluded.exchange,
                exchangeName = excluded.exchangeName,
                exchangeDataDelayedBy = excluded.exchangeDataDelayedBy,
                marketState = excluded.marketState,
                quoteType = excluded.quoteType,
                shortName = excluded.shortName,
                longName = excluded.longName,
                currency = excluded.currency,
                quoteSourceName = excluded.quoteSourceName,
                currencySymbol = excluded.currencySymbol,
                fromCurrency = excluded.fromCurrency,
                toCurrency = excluded.toCurrency,
                lastMarket = excluded.lastMarket,
                marketCap = excluded.marketCap,
                last_updated = CURRENT_TIMESTAMP
        `;

        const params = [
            symbol,
            priceData.preMarketSource || null,
            priceData.postMarketChangePercent || null,
            priceData.postMarketChange || null,
            priceData.postMarketTime || null,
            priceData.postMarketPrice || null,
            priceData.postMarketSource || null,
            priceData.regularMarketChangePercent || null,
            priceData.regularMarketChange || null,
            priceData.regularMarketTime || null,
            priceData.priceHint || null,
            priceData.regularMarketPrice || null,
            priceData.regularMarketDayHigh || null,
            priceData.regularMarketDayLow || null,
            priceData.regularMarketVolume || null,
            priceData.averageDailyVolume10Day || null,
            priceData.averageDailyVolume3Month || null,
            priceData.regularMarketPreviousClose || null,
            priceData.regularMarketSource || null,
            priceData.regularMarketOpen || null,
            priceData.exchange || null,
            priceData.exchangeName || null,
            priceData.exchangeDataDelayedBy || null,
            priceData.marketState || null,
            priceData.quoteType || null,
            priceData.shortName || null,
            priceData.longName || null,
            priceData.currency || null,
            priceData.quoteSourceName || null,
            priceData.currencySymbol || null,
            priceData.fromCurrency || null,
            priceData.toCurrency || null,
            priceData.lastMarket || null,
            priceData.marketCap || null,
        ];

        try {
            await this.db.run(query, ...params);
        } catch (error) {
            logger.error(`Failed to upsert price data for symbol: ${symbol}`);
            logger.error(`Query: ${query}`);
            logger.error(`Parameters: ${JSON.stringify(params, null, 2)}`);
            logger.error(`Error: ${error instanceof Error ? error.message : JSON.stringify(error)}`);
            throw error;
        }
    }

    public async upsertBalanceSheetHistory(symbol: string, balanceSheetStatements: any[]): Promise<void> {
        const query = `
            INSERT INTO balance_sheet_history (
                symbol,
                endDate_raw,
                endDate_fmt,
                last_updated
            )
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(symbol, endDate_raw) DO UPDATE SET
                endDate_fmt = excluded.endDate_fmt,
                last_updated = CURRENT_TIMESTAMP
        `;

        try {
            for (const statement of balanceSheetStatements) {
                const endDateRaw = statement.endDate?.raw || null;
                const endDateFmt = statement.endDate?.fmt || null;

                // Ensure endDate_raw is not null since it's part of UNIQUE constraint
                if (endDateRaw === null) {
                    logger.warn(`Skipping balance sheet statement with null endDate for symbol: ${symbol}`);
                    continue;
                }

                await this.db.run(query, [
                    symbol,
                    endDateRaw,
                    endDateFmt
                ]);
            }
        } catch (error) {
            logger.error(`Failed to upsert balance sheet history for symbol: ${symbol}`);
            logger.error(`Error: ${error instanceof Error ? error.message : JSON.stringify(error)}`);
            throw error;
        }
    }

    public async upsertEarningsTrend(symbol: string, trends: any[]): Promise<void> {
        const query = `
            INSERT INTO earnings_trend (
                symbol,
                period,
                endDate,
                growth,
                earnings_avg,
                earnings_low,
                earnings_high,
                yearAgoEps,
                earnings_analysts,
                earnings_growth,
                revenue_avg,
                revenue_low,
                revenue_high,
                yearAgoRevenue,
                revenue_analysts,
                revenue_growth,
                last_updated
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(symbol, period) DO UPDATE SET
                endDate = excluded.endDate,
                growth = excluded.growth,
                earnings_avg = excluded.earnings_avg,
                earnings_low = excluded.earnings_low,
                earnings_high = excluded.earnings_high,
                yearAgoEps = excluded.yearAgoEps,
                earnings_analysts = excluded.earnings_analysts,
                earnings_growth = excluded.earnings_growth,
                revenue_avg = excluded.revenue_avg,
                revenue_low = excluded.revenue_low,
                revenue_high = excluded.revenue_high,
                yearAgoRevenue = excluded.yearAgoRevenue,
                revenue_analysts = excluded.revenue_analysts,
                revenue_growth = excluded.revenue_growth,
                last_updated = CURRENT_TIMESTAMP
        `;

        try {
            for (const trend of trends) {
                const earningsEstimate = trend.earningsEstimate || {};
                const revenueEstimate = trend.revenueEstimate || {};

                await this.db.run(query, [
                    symbol,
                    trend.period || null,
                    trend.endDate || null,
                    trend.growth?.raw || null,
                    earningsEstimate.avg?.raw || null,
                    earningsEstimate.low?.raw || null,
                    earningsEstimate.high?.raw || null,
                    earningsEstimate.yearAgoEps?.raw || null,
                    earningsEstimate.numberOfAnalysts?.raw || null,
                    earningsEstimate.growth?.raw || null,
                    revenueEstimate.avg?.raw || null,
                    revenueEstimate.low?.raw || null,
                    revenueEstimate.high?.raw || null,
                    revenueEstimate.yearAgoRevenue?.raw || null,
                    revenueEstimate.numberOfAnalysts?.raw || null,
                    revenueEstimate.growth?.raw || null,
                ]);
            }
        } catch (error) {
            logger.error(`Failed to upsert earnings trend for symbol: ${symbol}`);
            logger.error(`Error: ${error instanceof Error ? error.message : JSON.stringify(error)}`);
            throw error;
        }
    }

    public async upsertInstitutionOwnership(symbol: string, ownershipList: any[]): Promise<void> {
        const query = `
            INSERT INTO institution_ownership (
                symbol,
                reportDate_raw,
                reportDate_fmt,
                organization,
                pctHeld_raw,
                pctHeld_fmt,
                position_raw,
                position_fmt,
                position_longFmt,
                value_raw,
                value_fmt,
                value_longFmt,
                pctChange_raw,
                pctChange_fmt,
                last_updated
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(symbol, reportDate_raw, organization) DO UPDATE SET
                reportDate_fmt = excluded.reportDate_fmt,
                pctHeld_raw = excluded.pctHeld_raw,
                pctHeld_fmt = excluded.pctHeld_fmt,
                position_raw = excluded.position_raw,
                position_fmt = excluded.position_fmt,
                position_longFmt = excluded.position_longFmt,
                value_raw = excluded.value_raw,
                value_fmt = excluded.value_fmt,
                value_longFmt = excluded.value_longFmt,
                pctChange_raw = excluded.pctChange_raw,
                pctChange_fmt = excluded.pctChange_fmt,
                last_updated = CURRENT_TIMESTAMP
        `;

        try {
            for (const ownership of ownershipList) {
                await this.db.run(query, [
                    symbol,
                    ownership.reportDate?.raw || null,
                    ownership.reportDate?.fmt || null,
                    ownership.organization || null,
                    ownership.pctHeld?.raw || null,
                    ownership.pctHeld?.fmt || null,
                    ownership.position?.raw || null,
                    ownership.position?.fmt || null,
                    ownership.position?.longFmt || null,
                    ownership.value?.raw || null,
                    ownership.value?.fmt || null,
                    ownership.value?.longFmt || null,
                    ownership.pctChange?.raw || null,
                    ownership.pctChange?.fmt || null,
                ]);
            }
        } catch (error) {
            logger.error(`Failed to upsert institution ownership for symbol: ${symbol}`);
            logger.error(`Error: ${error instanceof Error ? error.message : JSON.stringify(error)}`);
            throw error;
        }
    }

    public async upsertMajorHoldersBreakdown(symbol: string, breakdown: any): Promise<void> {
        const query = `
            INSERT INTO major_holders_breakdown (
                symbol,
                insidersPercentHeld,
                institutionsPercentHeld,
                institutionsFloatPercentHeld,
                institutionsCount,
                last_updated
            )
            VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(symbol) DO UPDATE SET
                insidersPercentHeld = excluded.insidersPercentHeld,
                institutionsPercentHeld = excluded.institutionsPercentHeld,
                institutionsFloatPercentHeld = excluded.institutionsFloatPercentHeld,
                institutionsCount = excluded.institutionsCount,
                last_updated = CURRENT_TIMESTAMP
        `;

        try {
            await this.db.run(query, [
                symbol,
                breakdown.insidersPercentHeld || null,
                breakdown.institutionsPercentHeld || null,
                breakdown.institutionsFloatPercentHeld || null,
                breakdown.institutionsCount || null,
            ]);
        } catch (error) {
            logger.error(`Failed to upsert major holders breakdown for symbol: ${symbol}`);
            logger.error(`Error: ${error instanceof Error ? error.message : JSON.stringify(error)}`);
            throw error;
        }
    }

    public async upsertBalanceSheetHistoryQuarterly(symbol: string, balanceSheetStatements: any[]): Promise<void> {
        const query = `
            INSERT INTO balance_sheet_history_quarterly (
                symbol, endDate_raw, endDate_fmt, maxAge, last_updated
            )
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(symbol, endDate_raw) DO UPDATE SET
                endDate_fmt = excluded.endDate_fmt,
                maxAge = excluded.maxAge,
                last_updated = CURRENT_TIMESTAMP
        `;

        try {
            for (const statement of balanceSheetStatements) {
                await this.db.run(query, [
                    symbol,
                    statement.endDate?.raw || null,
                    statement.endDate?.fmt || null,
                    statement.maxAge || null,
                ]);
            }
        } catch (error) {
            logger.error(`Failed to upsert balance sheet history quarterly for symbol: ${symbol}`);
            logger.error(`Error: ${error instanceof Error ? error.message : JSON.stringify(error)}`);
            throw error;
        }
    }

    public async upsertEarningsHistory(symbol: string, earningsHistory: any[]): Promise<void> {
        const query = `
            INSERT INTO earnings_history (
                symbol, quarter_raw, quarter_fmt, epsActual_raw, epsActual_fmt,
                epsEstimate_raw, epsEstimate_fmt, epsDifference_raw, epsDifference_fmt,
                surprisePercent_raw, surprisePercent_fmt, currency, period, maxAge, last_updated
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(symbol, quarter_raw) DO UPDATE SET
                quarter_fmt = excluded.quarter_fmt,
                epsActual_raw = excluded.epsActual_raw,
                epsActual_fmt = excluded.epsActual_fmt,
                epsEstimate_raw = excluded.epsEstimate_raw,
                epsEstimate_fmt = excluded.epsEstimate_fmt,
                epsDifference_raw = excluded.epsDifference_raw,
                epsDifference_fmt = excluded.epsDifference_fmt,
                surprisePercent_raw = excluded.surprisePercent_raw,
                surprisePercent_fmt = excluded.surprisePercent_fmt,
                currency = excluded.currency,
                period = excluded.period,
                maxAge = excluded.maxAge,
                last_updated = CURRENT_TIMESTAMP
        `;

        try {
            for (const history of earningsHistory) {
                await this.db.run(query, [
                    symbol,
                    history.quarter?.raw || null,
                    history.quarter?.fmt || null,
                    history.epsActual?.raw || null,
                    history.epsActual?.fmt || null,
                    history.epsEstimate?.raw || null,
                    history.epsEstimate?.fmt || null,
                    history.epsDifference?.raw || null,
                    history.epsDifference?.fmt || null,
                    history.surprisePercent?.raw || null,
                    history.surprisePercent?.fmt || null,
                    history.currency || null,
                    history.period || null,
                    history.maxAge || null,
                ]);
            }
        } catch (error) {
            logger.error(`Failed to upsert earnings history for symbol: ${symbol}`);
            logger.error(`Error: ${error instanceof Error ? error.message : JSON.stringify(error)}`);
            throw error;
        }
    }

    public async upsertMajorDirectHolders(symbol: string, majorDirectHolders: any): Promise<void> {
        const holdersJson = JSON.stringify(majorDirectHolders.holders || []);

        const query = `
            INSERT INTO major_direct_holders (
                symbol, maxAge, holders, last_updated
            )
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(symbol) DO UPDATE SET
                maxAge = excluded.maxAge,
                holders = excluded.holders,
                last_updated = CURRENT_TIMESTAMP
        `;

        try {
            // 데이터를 테이블에 삽입하거나 업데이트
            await this.db.run(query, [
                symbol,
                majorDirectHolders.maxAge || null,
                holdersJson
            ]);
        } catch (error) {
            // 에러 로깅
            logger.error(`Failed to upsert major direct holders for symbol: ${symbol}`);
            logger.error(`Error: ${error instanceof Error ? error.message : JSON.stringify(error)}`);
            throw error;
        }
    }

    public async upsertSummaryProfile(symbol: string, summaryProfile: any): Promise<void> {
        const companyOfficersJson = JSON.stringify(summaryProfile.companyOfficers || []);

        const query = `
            INSERT INTO summary_profile (
                symbol,
                address1,
                city,
                state,
                zip,
                country,
                phone,
                website,
                industry,
                industryKey,
                industryDisp,
                sector,
                sectorKey,
                sectorDisp,
                longBusinessSummary,
                fullTimeEmployees,
                companyOfficers,
                irWebsite,
                maxAge,
                last_updated
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
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
                irWebsite = excluded.irWebsite,
                maxAge = excluded.maxAge,
                last_updated = CURRENT_TIMESTAMP
        `;

        const params = [
            symbol,
            summaryProfile.address1 || null,
            summaryProfile.city || null,
            summaryProfile.state || null,
            summaryProfile.zip || null,
            summaryProfile.country || null,
            summaryProfile.phone || null,
            summaryProfile.website || null,
            summaryProfile.industry || null,
            summaryProfile.industryKey || null,
            summaryProfile.industryDisp || null,
            summaryProfile.sector || null,
            summaryProfile.sectorKey || null,
            summaryProfile.sectorDisp || null,
            summaryProfile.longBusinessSummary || null,
            summaryProfile.fullTimeEmployees || null,
            companyOfficersJson,
            summaryProfile.irWebsite || null,
            summaryProfile.maxAge || null,
        ];

        try {
            await this.db.run(query, ...params);
        } catch (error) {
            logger.error(`Failed to upsert summary profile for symbol: ${symbol}`);
            logger.error(`Error: ${error instanceof Error ? error.message : JSON.stringify(error)}`);
            throw error;
        }
    }

    public async upsertNetSharePurchaseActivity(symbol: string, activity: any): Promise<void> {
        const query = `
            INSERT INTO net_share_purchase_activity (
                symbol,
                maxAge,
                period,
                buyInfoCount,
                buyInfoShares,
                buyPercentInsiderShares,
                sellInfoCount,
                sellInfoShares,
                sellPercentInsiderShares,
                netInfoCount,
                netInfoShares,
                netPercentInsiderShares,
                totalInsiderShares,
                last_updated
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(symbol) DO UPDATE SET
                maxAge = excluded.maxAge,
                period = excluded.period,
                buyInfoCount = excluded.buyInfoCount,
                buyInfoShares = excluded.buyInfoShares,
                buyPercentInsiderShares = excluded.buyPercentInsiderShares,
                sellInfoCount = excluded.sellInfoCount,
                sellInfoShares = excluded.sellInfoShares,
                sellPercentInsiderShares = excluded.sellPercentInsiderShares,
                netInfoCount = excluded.netInfoCount,
                netInfoShares = excluded.netInfoShares,
                netPercentInsiderShares = excluded.netPercentInsiderShares,
                totalInsiderShares = excluded.totalInsiderShares,
                last_updated = CURRENT_TIMESTAMP
        `;

        const params = [
            symbol,
            activity.maxAge || null,
            activity.period || null,
            activity.buyInfoCount || null,
            activity.buyInfoShares || null,
            activity.buyPercentInsiderShares || null,
            activity.sellInfoCount || null,
            activity.sellInfoShares || null,
            activity.sellPercentInsiderShares || null,
            activity.netInfoCount || null,
            activity.netInfoShares || null,
            activity.netPercentInsiderShares || null,
            activity.totalInsiderShares || null,
        ];

        try {
            await this.db.run(query, ...params);
        } catch (error) {
            logger.error(`Failed to upsert net share purchase activity for symbol: ${symbol}`);
            logger.error(`Error: ${error instanceof Error ? error.message : JSON.stringify(error)}`);
            throw error;
        }
    }

    public async upsertInsiderTransactions(symbol: string, transactions: any[]): Promise<void> {
        const query = `
            INSERT INTO insider_transactions (
                symbol,
                filerName,
                startDate,
                shares_raw,
                shares_fmt,
                shares_longFmt,
                value_raw,
                value_fmt,
                value_longFmt,
                filerRelation,
                transactionText,
                ownership,
                maxAge,
                last_updated
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(symbol, filerName, startDate) DO UPDATE SET
                shares_raw = excluded.shares_raw,
                shares_fmt = excluded.shares_fmt,
                shares_longFmt = excluded.shares_longFmt,
                value_raw = excluded.value_raw,
                value_fmt = excluded.value_fmt,
                value_longFmt = excluded.value_longFmt,
                filerRelation = excluded.filerRelation,
                transactionText = excluded.transactionText,
                ownership = excluded.ownership,
                maxAge = excluded.maxAge,
                last_updated = CURRENT_TIMESTAMP
        `;

        try {
            const insertPromises = transactions.map(transaction => {
                const params = [
                    symbol,
                    transaction.filerName || null,
                    transaction.startDate?.raw || null,
                    transaction.shares?.raw || null,
                    transaction.shares?.fmt || null,
                    transaction.shares?.longFmt || null,
                    transaction.value?.raw || null,
                    transaction.value?.fmt || null,
                    transaction.value?.longFmt || null,
                    transaction.filerRelation || null,
                    transaction.transactionText || null,
                    transaction.ownership || null,
                    transaction.maxAge || null,
                ];
                return this.db.run(query, ...params);
            });

            await Promise.all(insertPromises);
        } catch (error) {
            logger.error(`Failed to upsert insider transactions for symbol: ${symbol}`);
            logger.error(`Error: ${error instanceof Error ? error.message : JSON.stringify(error)}`);
            throw error;
        }
    }

    public async upsertSectorTrend(symbol: string | null, sectorTrend: { maxAge?: number; estimates: any[] }): Promise<void> {
        const query = `
            INSERT INTO sector_trend (
                symbol,
                maxAge,
                estimate,
                last_updated
            )
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
        `;

        try {
            const maxAge = sectorTrend?.maxAge || null;
            const estimates: any[] = sectorTrend?.estimates || []; // 타입 명시

            const insertPromises = estimates.map((estimate: any) => {
                const params = [
                    symbol,
                    maxAge,
                    JSON.stringify(estimate),
                ];
                return this.db.run(query, ...params);
            });

            // If there are no estimates, insert a single record with null estimate
            if (estimates.length === 0) {
                const params = [
                    symbol,
                    maxAge,
                    null,
                ];
                await this.db.run(query, ...params);
            } else {
                await Promise.all(insertPromises);
            }
        } catch (error) {
            logger.error(`Failed to upsert sector trend for symbol: ${symbol || 'null'}`);
            logger.error(`Error: ${error instanceof Error ? error.message : JSON.stringify(error)}`);
            throw error;
        }
    }

    public async upsertIncomeStatementHistoryQuarterly(
        symbol: string,
        incomeStatementHistoryQuarterly: { incomeStatementHistory: any[]; maxAge?: number }
    ): Promise<void> {
        const query = `
            INSERT INTO income_statement_history_quarterly (
                symbol,
                endDate_raw,
                endDate_fmt,
                totalRevenue_raw,
                totalRevenue_fmt,
                totalRevenue_longFmt,
                costOfRevenue_raw,
                costOfRevenue_fmt,
                costOfRevenue_longFmt,
                grossProfit_raw,
                grossProfit_fmt,
                grossProfit_longFmt,
                ebit_raw,
                ebit_fmt,
                ebit_longFmt,
                incomeTaxExpense_raw,
                incomeTaxExpense_fmt,
                incomeTaxExpense_longFmt,
                netIncome_raw,
                netIncome_fmt,
                netIncome_longFmt,
                last_updated
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(symbol, endDate_raw) DO UPDATE SET
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
                last_updated = CURRENT_TIMESTAMP;
        `;

        try {
            const incomeStatements = incomeStatementHistoryQuarterly.incomeStatementHistory || [];

            for (const statement of incomeStatements) {
                const params = [
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
                ];

                await this.db.run(query, params);
            }
        } catch (error) {
            logger.error(`Failed to upsert income statement history quarterly for symbol: ${symbol}`);
            logger.error(error instanceof Error ? error.message : JSON.stringify(error));
            throw error;
        }
    }

    public async upsertCashflowStatementHistoryQuarterly(symbol: string, cashflowStatements: any[]): Promise<void> {
        const query = `
            INSERT INTO cashflow_statement_history_quarterly (
                symbol, end_date, net_income_raw, net_income_fmt, net_income_long_fmt
            )
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(symbol, end_date) DO UPDATE SET
                net_income_raw = excluded.net_income_raw,
                net_income_fmt = excluded.net_income_fmt,
                net_income_long_fmt = excluded.net_income_long_fmt
        `;

        try {
            for (const statement of cashflowStatements) {
                const params = [
                    symbol,
                    statement?.endDate?.raw || null,
                    statement?.netIncome?.raw || null,
                    statement?.netIncome?.fmt || null,
                    statement?.netIncome?.longFmt || null,
                ];

                await this.db.run(query, params);
            }
        } catch (error) {
            logger.error(`Failed to upsert cashflow statement history quarterly for symbol: ${symbol}`);
            logger.error(error instanceof Error ? error.message : JSON.stringify(error));
            throw error;
        }
    }

    public async upsertEarningsChart(symbol: string, quarterlyEarnings: any[]): Promise<void> {
        const query = `
            INSERT INTO earnings_chart (
                symbol, date, actual, estimate, last_updated
            )
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(symbol, date) DO UPDATE SET
                actual = excluded.actual,
                estimate = excluded.estimate,
                last_updated = CURRENT_TIMESTAMP;
        `;

        try {
            for (const earnings of quarterlyEarnings) {
                await this.db.run(query, [
                    symbol,
                    earnings.date,
                    earnings.actual || null,
                    earnings.estimate || null
                ]);
            }
        } catch (error) {
            logger.error(`Failed to upsert earnings chart for symbol: ${symbol}`);
            logger.error(`Error: ${error instanceof Error ? error.message : JSON.stringify(error)}`);
            throw error;
        }
    }

    public async upsertCurrentQuarterEstimate(estimate: { symbol: string; estimate: number | null; date: string | null; year: number | null }): Promise<void> {
        const query = `
            INSERT INTO current_quarter_estimate (
                symbol, current_estimate, estimate_date, estimate_year, last_updated
            )
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(symbol) DO UPDATE SET
                current_estimate = excluded.current_estimate,
                estimate_date = excluded.estimate_date,
                estimate_year = excluded.estimate_year,
                last_updated = CURRENT_TIMESTAMP
        `;

        const params = [
            estimate.symbol,
            estimate.estimate,
            estimate.date,
            estimate.year,
        ];

        try {
            await this.db.run(query, ...params);
        } catch (error) {
            logger.error(`Failed to upsert current quarter estimate for symbol: ${estimate.symbol}`);
            logger.error(`Error: ${error instanceof Error ? error.message : JSON.stringify(error)}`);
            throw error;
        }
    }

    public async upsertEarningsDate(symbol: string, earningsDate: any): Promise<void> {
        const query = `
            INSERT INTO earnings_date (
                symbol, earnings_date, is_estimate, last_updated
            )
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(symbol) DO UPDATE SET
                earnings_date = excluded.earnings_date,
                is_estimate = excluded.is_estimate,
                last_updated = CURRENT_TIMESTAMP;
        `;

        try {
            await this.db.run(query, [
                symbol,
                earningsDate.earningsDate,
                earningsDate.isEstimate ? 1 : 0
            ]);
        } catch (error) {
            logger.error(`Failed to upsert earnings date for symbol: ${symbol}`);
            logger.error(error);
            throw error;
        }
    }

    public async upsertFinancialChartYearly(symbol: string, yearlyData: any[]): Promise<void> {
        const query = `
            INSERT INTO financial_chart_yearly (
                symbol, date, revenue, earnings, last_updated
            )
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(symbol, date) DO UPDATE SET
                revenue = excluded.revenue,
                earnings = excluded.earnings,
                last_updated = CURRENT_TIMESTAMP;
        `;

        try {
            // Validate and process each year's data
            for (const data of yearlyData) {
                const date = data.date;
                const revenue = data.revenue || null;
                const earnings = data.earnings || null;

                try {
                    await this.db.run(query, [symbol, date, revenue, earnings]);
                    logger.info(`Upserted financial chart yearly record for symbol: ${symbol}, date: ${date}`);
                } catch (insertError) {
                    logger.error(`Failed to upsert record for symbol: ${symbol}, date: ${date}`);
                    logger.error(`Data: ${JSON.stringify(data)}`);
                    logger.error(`Error: ${insertError instanceof Error ? insertError.message : JSON.stringify(insertError)}`);
                }
            }
        } catch (error) {
            logger.error(`Failed to process financial chart yearly data for symbol: ${symbol}`);
            logger.error(`Data: ${JSON.stringify(yearlyData, null, 2)}`);
            logger.error(`Error: ${error instanceof Error ? error.message : JSON.stringify(error)}`);
            throw error;
        }
    }

    public async upsertFinancialChartQuarterly(symbol: string, quarterlyData: any[]): Promise<void> {
        const query = `
            INSERT INTO financial_chart_quarterly (
                symbol, date, revenue, earnings, last_updated
            )
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(symbol, date) DO UPDATE SET
                revenue = excluded.revenue,
                earnings = excluded.earnings,
                last_updated = CURRENT_TIMESTAMP;
        `;

        try {
            // Validate and process each quarter's data
            for (const data of quarterlyData) {
                const date = data.date; // Extract the date
                const revenue = data.revenue || null; // Default to null if missing
                const earnings = data.earnings || null; // Default to null if missing

                try {
                    await this.db.run(query, [symbol, date, revenue, earnings]);
                    logger.info(`Upserted financial chart quarterly record for symbol: ${symbol}, date: ${date}`);
                } catch (insertError) {
                    logger.error(`Failed to upsert record for symbol: ${symbol}, date: ${date}`);
                    logger.error(`Data: ${JSON.stringify(data)}`);
                    logger.error(`Error: ${insertError instanceof Error ? insertError.message : JSON.stringify(insertError)}`);
                }
            }
        } catch (error) {
            logger.error(`Failed to process financial chart quarterly data for symbol: ${symbol}`);
            logger.error(`Data: ${JSON.stringify(quarterlyData, null, 2)}`);
            logger.error(`Error: ${error instanceof Error ? error.message : JSON.stringify(error)}`);
            throw error;
        }
    }

    public async upsertFinancialData(symbol: string, financialData: any): Promise<void> {
        const query = `
            INSERT INTO financial_data (
                symbol, current_price, target_high_price, target_low_price, target_mean_price,
                target_median_price, recommendation_mean, recommendation_key, number_of_analyst_opinions,
                total_cash, total_cash_per_share, ebitda, total_debt, quick_ratio, current_ratio,
                total_revenue, debt_to_equity, revenue_per_share, return_on_assets, return_on_equity,
                gross_profits, free_cashflow, operating_cashflow, earnings_growth, revenue_growth, gross_margins,
                ebitda_margins, operating_margins, profit_margins, financial_currency
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol) DO UPDATE SET
                current_price = excluded.current_price,
                target_high_price = excluded.target_high_price,
                target_low_price = excluded.target_low_price,
                target_mean_price = excluded.target_mean_price,
                target_median_price = excluded.target_median_price,
                recommendation_mean = excluded.recommendation_mean,
                recommendation_key = excluded.recommendation_key,
                number_of_analyst_opinions = excluded.number_of_analyst_opinions,
                total_cash = excluded.total_cash,
                total_cash_per_share = excluded.total_cash_per_share,
                ebitda = excluded.ebitda,
                total_debt = excluded.total_debt,
                quick_ratio = excluded.quick_ratio,
                current_ratio = excluded.current_ratio,
                total_revenue = excluded.total_revenue,
                debt_to_equity = excluded.debt_to_equity,
                revenue_per_share = excluded.revenue_per_share,
                return_on_assets = excluded.return_on_assets,
                return_on_equity = excluded.return_on_equity,
                gross_profits = excluded.gross_profits,
                free_cashflow = excluded.free_cashflow,
                operating_cashflow = excluded.operating_cashflow,
                earnings_growth = excluded.earnings_growth,
                revenue_growth = excluded.revenue_growth,
                gross_margins = excluded.gross_margins,
                ebitda_margins = excluded.ebitda_margins,
                operating_margins = excluded.operating_margins,
                profit_margins = excluded.profit_margins,
                financial_currency = excluded.financial_currency,
                last_updated = CURRENT_TIMESTAMP;
        `;

        const params = [
            symbol,
            financialData.currentPrice ?? null,
            financialData.targetHighPrice ?? null,
            financialData.targetLowPrice ?? null,
            financialData.targetMeanPrice ?? null,
            financialData.targetMedianPrice ?? null,
            financialData.recommendationMean ?? null,
            financialData.recommendationKey ?? null,
            financialData.numberOfAnalystOpinions ?? null,
            financialData.totalCash ?? null,
            financialData.totalCashPerShare ?? null,
            financialData.ebitda ?? null,
            financialData.totalDebt ?? null,
            financialData.quickRatio ?? null,
            financialData.currentRatio ?? null,
            financialData.totalRevenue ?? null,
            financialData.debtToEquity ?? null,
            financialData.revenuePerShare ?? null,
            financialData.returnOnAssets ?? null,
            financialData.returnOnEquity ?? null,
            financialData.grossProfits ?? null,
            financialData.freeCashflow ?? null,
            financialData.operatingCashflow ?? null,
            financialData.earningsGrowth ?? null,
            financialData.revenueGrowth ?? null,
            financialData.grossMargins ?? null,
            financialData.ebitdaMargins ?? null,
            financialData.operatingMargins ?? null,
            financialData.profitMargins ?? null,
            financialData.financialCurrency ?? null,
        ];

        try {
            await this.db.run(query, ...params);
        } catch (error) {
            logger.error(`Failed to upsert financial data for symbol: ${symbol}`);
            logger.error(`Query: ${query}`);
            logger.error(`Parameters: ${JSON.stringify(params, null, 2)}`);
            logger.error(`Error: ${error instanceof Error ? error.message : JSON.stringify(error)}`);
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
