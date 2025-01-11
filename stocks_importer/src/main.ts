import yahooFinance from 'yahoo-finance2';
import { StockScannerService } from './services/StockScannerService.js';
import logger from './services/LoggerService.js';
import { DBService } from './services/DBService.js';

class Main {
    public static async main(): Promise<void> {
        logger.info('################### Main Start !! #######################');

        // Suppress Yahoo Finance API notices
        yahooFinance.suppressNotices(['yahooSurvey']);

        const stockScannerService = new StockScannerService();
        const dbService = new DBService();
        await dbService.initialize();

        try {
            // Step 1. Fetch all the list of symbols from yahoo finance API
            //const allStocks = await stockScannerService.fetchStocksAll();

            const allStocks = [{ symbol: 'AAPL', categoryId: '', categoryName: '' }];

            // ex)
            // { "symbol": "WBA", "categoryId": "day_gainers", "categoryName": "Day Gainers" }

            // Step 2 : insert the basic information into DB
            // for (const stock of allStocks) {
            //     await dbService.upsertSymbolAndCategory(stock.symbol, stock.categoryId, stock.categoryName);

            //     logger.info(`Inserted/Updated: ${stock.symbol} in category ${stock.categoryName}`);
            // }



            // step 3: Call the Yahoo Finance API to retrieve detailed information for each stock
            for (const stock of allStocks) {
                try {
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
                } catch (error) {
                    logger.error(`Failed to fetch or upsert asset profile for symbol: ${stock.symbol}. Error: ${error}`);
                }
            }

            logger.info(`Fetched and processed ${allStocks.length} stock symbols.`);
        } catch (error) {
            logger.error(`Error during fetchStocksAll: ${error}`);
        }

        await dbService.close();

        logger.info('################### Main End !! #######################');
    }
}

Main.main();
