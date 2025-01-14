import fs from 'fs';
import path from 'path';
import yahooFinance from 'yahoo-finance2';
import { StockScannerService } from './services/StockScannerService.js';
import logger from './services/LoggerService.js';
import { DBService } from './services/DBService.js';

class Main {
    static async loadWatchlists(baseFolder: string) {
        const watchlist = [];

        try {
            const categories = fs.readdirSync(baseFolder, { withFileTypes: true })
                .filter(dirent => dirent.isDirectory())
                .map(dirent => dirent.name);

            for (const category of categories) {
                const categoryFolder = path.join(baseFolder, category);
                const filePath = path.join(categoryFolder, 'data.json');

                if (fs.existsSync(filePath)) {
                    try {
                        const data = JSON.parse(fs.readFileSync(filePath, 'utf8'));

                        if (data.watchlist && Array.isArray(data.watchlist)) {
                            for (const symbol of data.watchlist) {
                                watchlist.push({
                                    symbol,
                                    categoryId: category,
                                    categoryName: category.replace(/_/g, ' ')
                                });
                            }
                        }
                    } catch (error) {
                        logger.error(`Failed to load or parse JSON from ${filePath}:`, error);
                    }
                } else {
                    logger.warn(`File not found: ${filePath}`);
                }
            }
        } catch (error) {
            logger.error(`Failed to read categories from ${baseFolder}:`, error);
        }

        return watchlist;
    }

    public static async main(): Promise<void> {
        logger.info('################### Main Start !! #######################');

        // Suppress Yahoo Finance API notices
        yahooFinance.suppressNotices(['yahooSurvey']);

        const stockScannerService = new StockScannerService();
        const dbService = new DBService();
        await dbService.initialize();

        try {
            // Step 1. Fetch watchlist
            const watchlist = await this.loadWatchlists('./watchlist');

            // Step 2. Fetch all the list of symbols from yahoo finance API
            const fetchedStocks = await stockScannerService.fetchStocksAll();
            const allStocks = [...watchlist, ...fetchedStocks]; // [{ symbol: 'AAPL', categoryId: '', categoryName: '' }];

            // Step 3 : Insert the basic information into DB
            for (const stock of allStocks) {
                await dbService.upsertStock(stock.symbol, stock.categoryId, stock.categoryName);

                logger.info(`Inserted/Updated: ${stock.symbol} in category ${stock.categoryName}`);
            }

            // step 4: Call the Yahoo Finance API to retrieve detailed information for each stock
            for (const stock of allStocks) {
                try {
                    const delay = Math.floor(Math.random() * 6000) + 5000; // Random delay between 5 and 10 seconds
                    await new Promise(resolve => setTimeout(resolve, delay));

                    const stockInfo = await stockScannerService.fetchStockDetails(stock.symbol);

                    // Extract the assetProfile if it exists
                    const assetProfile = stockInfo?.assetProfile;
                    if (assetProfile) {
                        await dbService.upsertAssetProfile(stock.symbol, assetProfile);
                        logger.info(`Upserted asset profile for symbol: ${stock.symbol}`);
                    } else {
                        logger.warn(`No asset profile found for symbol: ${stock.symbol}`);
                    }

                    // Extract the recommendationTrend if it exists
                    const recommendationTrend = stockInfo?.recommendationTrend;
                    if (recommendationTrend?.trend) {
                        await dbService.upsertRecommendationTrend(stock.symbol, recommendationTrend.trend);
                        logger.info(`Upserted recommendation trend for symbol: ${stock.symbol}`);
                    } else {
                        logger.warn(`No recommendation trend data found for symbol: ${stock.symbol}`);
                    }

                    // Extract the cashflowStatementHistory if it exists
                    const cashflowStatementHistory = stockInfo?.cashflowStatementHistory?.cashflowStatements;
                    if (cashflowStatementHistory) {
                        await dbService.upsertCashflowStatementHistory(stock.symbol, cashflowStatementHistory);
                        logger.info(`Upserted cashflow statement history for symbol: ${stock.symbol}`);
                    } else {
                        logger.warn(`No cashflow statement history data found for symbol: ${stock.symbol}`);
                    }

                    // Extract the indexTrend if it exists
                    const indexTrend = stockInfo?.indexTrend?.estimates;
                    if (indexTrend) {
                        await dbService.upsertIndexTrend(stock.symbol, indexTrend);
                        logger.info(`Upserted index trend for symbol: ${stock.symbol}`);
                    } else {
                        logger.warn(`No index trend data found for symbol: ${stock.symbol}`);
                    }

                    // Extract the defaultKeyStatistics if it exists
                    const defaultKeyStatistics = stockInfo?.defaultKeyStatistics;
                    if (defaultKeyStatistics) {
                        await dbService.upsertDefaultKeyStatistics(stock.symbol, defaultKeyStatistics);
                        logger.info(`Upserted default key statistics for symbol: ${stock.symbol}`);
                    } else {
                        logger.warn(`No default key statistics data found for symbol: ${stock.symbol}`);
                    }

                    // Extract the incomeStatementHistory if it exists
                    const incomeStatementHistory = stockInfo?.incomeStatementHistory?.incomeStatementHistory;
                    if (incomeStatementHistory) {
                        await dbService.upsertIncomeStatementHistory(stock.symbol, incomeStatementHistory);
                        logger.info(`Upserted income statement history for symbol: ${stock.symbol}`);
                    } else {
                        logger.warn(`No income statement history data found for symbol: ${stock.symbol}`);
                    }

                    // Extract the fundOwnership if it exists
                    const fundOwnership = stockInfo?.fundOwnership?.ownershipList;
                    if (fundOwnership) {
                        await dbService.upsertFundOwnership(stock.symbol, fundOwnership);
                        logger.info(`Upserted fund ownership for symbol: ${stock.symbol}`);
                    } else {
                        logger.warn(`No fund ownership data found for symbol: ${stock.symbol}`);
                    }

                    // Extract and upsert `summaryDetail`
                    const summaryDetail = stockInfo?.summaryDetail;
                    if (summaryDetail) {
                        await dbService.upsertSummaryDetail(stock.symbol, summaryDetail);
                        logger.info(`Upserted summary detail for symbol: ${stock.symbol}`);
                    } else {
                        logger.warn(`No summary detail data found for symbol: ${stock.symbol}`);
                    }

                    // Extract and upsert insiderHolders
                    const insiderHolders = stockInfo?.insiderHolders?.holders;
                    if (insiderHolders) {
                        await dbService.upsertInsiderHolders(stock.symbol, insiderHolders);
                        logger.info(`Upserted insider holders for symbol: ${stock.symbol}`);
                    } else {
                        logger.warn(`No insider holders data found for symbol: ${stock.symbol}`);
                    }

                    // Extract and upsert calendarEvents
                    const calendarEvents = stockInfo?.calendarEvents;
                    if (calendarEvents) {
                        await dbService.upsertCalendarEvents(stock.symbol, calendarEvents);
                        logger.info(`Upserted calendar events for symbol: ${stock.symbol}`);
                    } else {
                        logger.warn(`No calendar events data found for symbol: ${stock.symbol}`);
                    }

                    // Extract and upsert upgradeDowngradeHistory
                    const upgradeDowngradeHistory = stockInfo?.upgradeDowngradeHistory?.history;
                    if (upgradeDowngradeHistory) {
                        await dbService.upsertUpgradeDowngradeHistory(stock.symbol, upgradeDowngradeHistory);
                        logger.info(`Upserted upgrade/downgrade history for symbol: ${stock.symbol}`);
                    } else {
                        logger.warn(`No upgrade/downgrade history data found for symbol: ${stock.symbol}`);
                    }

                    // Extract and upsert price
                    const price = stockInfo?.price;
                    if (price) {
                        await dbService.upsertPrice(stock.symbol, price);
                        logger.info(`Upserted price data for symbol: ${stock.symbol}`);
                    } else {
                        logger.warn(`No price data found for symbol: ${stock.symbol}`);
                    }

                    // Extract and upsert balanceSheetHistory
                    const balanceSheetHistory = stockInfo?.balanceSheetHistory?.balanceSheetStatements;
                    if (balanceSheetHistory) {
                        await dbService.upsertBalanceSheetHistory(stock.symbol, balanceSheetHistory);
                        logger.info(`Upserted balance sheet history for symbol: ${stock.symbol}`);
                    } else {
                        logger.warn(`No balance sheet history data found for symbol: ${stock.symbol}`);
                    }

                    // Extract and upsert earningsTrend
                    const earningsTrend = stockInfo?.earningsTrend?.trend;
                    if (earningsTrend) {
                        await dbService.upsertEarningsTrend(stock.symbol, earningsTrend);
                        logger.info(`Upserted earnings trend for symbol: ${stock.symbol}`);
                    } else {
                        logger.warn(`No earnings trend data found for symbol: ${stock.symbol}`);
                    }

                    // Extract and upsert institutionOwnership
                    const institutionOwnership = stockInfo?.institutionOwnership?.ownershipList;
                    if (institutionOwnership) {
                        await dbService.upsertInstitutionOwnership(stock.symbol, institutionOwnership);
                        logger.info(`Upserted institution ownership for symbol: ${stock.symbol}`);
                    } else {
                        logger.warn(`No institution ownership data found for symbol: ${stock.symbol}`);
                    }

                    // Extract and upsert majorHoldersBreakdown
                    const majorHoldersBreakdown = stockInfo?.majorHoldersBreakdown;
                    if (majorHoldersBreakdown) {
                        await dbService.upsertMajorHoldersBreakdown(stock.symbol, majorHoldersBreakdown);
                        logger.info(`Upserted major holders breakdown for symbol: ${stock.symbol}`);
                    } else {
                        logger.warn(`No major holders breakdown data found for symbol: ${stock.symbol}`);
                    }

                    // Extract and upsert balanceSheetHistoryQuarterly
                    const balanceSheetHistoryQuarterly = stockInfo?.balanceSheetHistoryQuarterly?.balanceSheetStatements;
                    if (balanceSheetHistoryQuarterly) {
                        await dbService.upsertBalanceSheetHistoryQuarterly(stock.symbol, balanceSheetHistoryQuarterly);
                        logger.info(`Upserted balance sheet history quarterly for symbol: ${stock.symbol}`);
                    } else {
                        logger.warn(`No balance sheet history quarterly data found for symbol: ${stock.symbol}`);
                    }

                    // Extract and upsert earningsHistory
                    const earningsHistory = stockInfo?.earningsHistory?.history;
                    if (earningsHistory) {
                        await dbService.upsertEarningsHistory(stock.symbol, earningsHistory);
                        logger.info(`Upserted earnings history for symbol: ${stock.symbol}`);
                    } else {
                        logger.warn(`No earnings history data found for symbol: ${stock.symbol}`);
                    }

                    // Extract and upsert majorDirectHolders
                    const majorDirectHolders = stockInfo?.majorDirectHolders;
                    if (majorDirectHolders) {
                        await dbService.upsertMajorDirectHolders(stock.symbol, majorDirectHolders);
                        logger.info(`Upserted major direct holders for symbol: ${stock.symbol}`);
                    } else {
                        logger.warn(`No major direct holders data found for symbol: ${stock.symbol}`);
                    }

                    // Extract and upsert summaryProfile
                    const summaryProfile = stockInfo?.summaryProfile;
                    if (summaryProfile) {
                        await dbService.upsertSummaryProfile(stock.symbol, summaryProfile);
                        logger.info(`Upserted summary profile for symbol: ${stock.symbol}`);
                    } else {
                        logger.warn(`No summary profile data found for symbol: ${stock.symbol}`);
                    }

                    // Extract and upsert netSharePurchaseActivity
                    const netSharePurchaseActivity = stockInfo?.netSharePurchaseActivity;
                    if (netSharePurchaseActivity) {
                        await dbService.upsertNetSharePurchaseActivity(stock.symbol, netSharePurchaseActivity);
                        logger.info(`Upserted net share purchase activity for symbol: ${stock.symbol}`);
                    } else {
                        logger.warn(`No net share purchase activity data found for symbol: ${stock.symbol}`);
                    }

                    // Extract and upsert insiderTransactions
                    const insiderTransactions = stockInfo?.insiderTransactions?.transactions;
                    if (insiderTransactions && insiderTransactions.length > 0) {
                        await dbService.upsertInsiderTransactions(stock.symbol, insiderTransactions);
                        logger.info(`Upserted insider transactions for symbol: ${stock.symbol}`);
                    } else {
                        logger.warn(`No insider transactions data found for symbol: ${stock.symbol}`);
                    }

                    // Extract and upsert sectorTrend
                    const sectorTrend = stockInfo?.sectorTrend;
                    if (sectorTrend) {
                        await dbService.upsertSectorTrend(stock.symbol, sectorTrend);
                        logger.info(`Upserted sector trend for symbol: ${stock.symbol || 'null'}`);
                    } else {
                        logger.warn(`No sector trend data found for symbol: ${stock.symbol || 'null'}`);
                    }

                    // Extract `incomeStatementHistoryQuarterly` and upsert to DB
                    const incomeStatementHistoryQuarterly = stockInfo?.incomeStatementHistoryQuarterly?.incomeStatementHistory;
                    if (incomeStatementHistoryQuarterly) {
                        await dbService.upsertIncomeStatementHistoryQuarterly(stock.symbol, incomeStatementHistoryQuarterly);
                        logger.info(`Upserted income statement history quarterly for symbol: ${stock.symbol}`);
                    } else {
                        logger.warn(`No income statement history quarterly data found for symbol: ${stock.symbol}`);
                    }

                    // Extract `cashflowStatementHistoryQuarterly` and upsert to DB
                    const cashflowStatementHistoryQuarterly = stockInfo?.cashflowStatementHistoryQuarterly?.cashflowStatements;
                    if (cashflowStatementHistoryQuarterly) {
                        await dbService.upsertCashflowStatementHistoryQuarterly(stock.symbol, cashflowStatementHistoryQuarterly);
                        logger.info(`Upserted cashflow statement history quarterly for symbol: ${stock.symbol}`);
                    } else {
                        logger.warn(`No cashflow statement history quarterly data found for symbol: ${stock.symbol}`);
                    }

                    // Extract and upsert `earnings` data
                    const earnings = stockInfo?.earnings;
                    if (earnings) {
                        const earningsChart = earnings.earningsChart || {};
                        const financialsChart = earnings.financialsChart || {};

                        // Upsert quarterly earnings data
                        if (earningsChart.quarterly) {
                            const quarterlyEarnings = earningsChart.quarterly.map((q: any) => ({
                                symbol: stock.symbol,
                                date: q.date,
                                actual: q.actual,
                                estimate: q.estimate,
                            }));
                            await dbService.upsertEarningsChart(stock.symbol, quarterlyEarnings); // Match with DB function name
                            logger.info(`Upserted quarterly earnings chart for symbol: ${stock.symbol}`);
                        } else {
                            logger.warn(`No quarterly earnings data found for symbol: ${stock.symbol}`);
                        }

                        // Upsert yearly financial data
                        if (financialsChart.yearly) {
                            const yearlyFinancials = financialsChart.yearly.map((y: any) => ({
                                symbol: stock.symbol,
                                date: y.date,
                                revenue: y.revenue,
                                earnings: y.earnings,
                            }));

                            await dbService.upsertFinancialChartYearly(stock.symbol, yearlyFinancials);
                            logger.info(`Upserted yearly financial chart for symbol: ${stock.symbol}`);
                        } else {
                            logger.warn(`No yearly financial data found for symbol: ${stock.symbol}`);
                        }

                        // Upsert quarterly financial data
                        if (financialsChart.quarterly) {
                            const quarterlyFinancials = financialsChart.quarterly.map((q: any) => ({
                                symbol: stock.symbol,
                                date: q.date,
                                revenue: q.revenue,
                                earnings: q.earnings,
                            }));
                            await dbService.upsertFinancialChartQuarterly(stock.symbol, quarterlyFinancials); // Match with DB function name
                            logger.info(`Upserted quarterly financial chart for symbol: ${stock.symbol}`);
                        } else {
                            logger.warn(`No quarterly financial data found for symbol: ${stock.symbol}`);
                        }

                        // Upsert current quarter estimate data
                        const currentEstimate = {
                            symbol: stock.symbol,
                            estimate: earningsChart.currentQuarterEstimate || null,
                            date: earningsChart.currentQuarterEstimateDate || null,
                            year: earningsChart.currentQuarterEstimateYear || null,
                        };
                        await dbService.upsertCurrentQuarterEstimate(currentEstimate); // Match with DB function name
                        logger.info(`Upserted current quarter estimate for symbol: ${stock.symbol}`);
                    } else {
                        logger.warn(`No earnings data found for symbol: ${stock.symbol}`);
                    }

                    const financialData = stockInfo?.financialData;
                    if (financialData) {
                        try {
                            await dbService.upsertFinancialData(stock.symbol, financialData);
                            logger.info(`Upserted financial data for symbol: ${stock.symbol}`);
                        } catch (error) {
                            logger.error(`Failed to upsert financial data for symbol: ${stock.symbol}`);
                            logger.error(`Data: ${JSON.stringify(financialData, null, 2)}`);
                            logger.error(error);
                            process.exit(1);
                        }
                    } else {
                        logger.warn(`No financial data found for symbol: ${stock.symbol}`);
                    }

                } catch (error) {
                    logger.error(`Failed to fetch or upsert asset profile for symbol: ${stock.symbol}. Error: ${error}`);
                    process.exit(1);
                }
            }

            logger.info(`Fetched and processed ${allStocks.length} stock symbols.`);
        } catch (error) {
            logger.error(`Error during fetchStocksAll: ${error}`);
            process.exit(1);
        }

        await dbService.close();

        logger.info('################### Main End !! #######################');
    }
}

Main.main();
