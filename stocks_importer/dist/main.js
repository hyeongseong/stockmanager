import yahooFinance from 'yahoo-finance2';
import { StockSymbolScannerService } from './services/StockSymbolScannerService.js';
import LoggerService from './services/LoggerService.js';
import { DBService } from './services/DBService.js';
class Main {
    static async main() {
        LoggerService.info('################### Main Start !! #######################');
        // Suppress Yahoo Finance API notices
        yahooFinance.suppressNotices(['yahooSurvey']);
        const stockService = new StockSymbolScannerService();
        const dbService = new DBService();
        await dbService.initialize();
        try {
            const allStocks = await stockService.fetchStocksAll();
            for (const stock of allStocks) {
                await dbService.upsertCategory(stock.symbol, stock.categoryId, stock.categoryName);
                LoggerService.info(`Inserted/Updated: ${stock.symbol} in category ${stock.categoryName}`);
            }
            LoggerService.info(`Fetched and processed ${allStocks.length} stock symbols.`);
        }
        catch (error) {
            LoggerService.error(`Error during fetchStocksAll: ${error}`);
        }
        await dbService.close();
        LoggerService.info('################### Main End !! #######################');
    }
}
Main.main();
