import yahooFinance from "yahoo-finance2";
import logger from './LoggerService.js';
export const SCR_IDS = {
    DAY_GAINERS: { id: "day_gainers", name: "Day Gainers" },
    DAY_LOSERS: { id: "day_losers", name: "Day Losers" },
    MOST_ACTIVES: { id: "most_actives", name: "Most Actives" },
    HIGH_YIELD_BOND: { id: "high_yield_bond", name: "High Yield Bonds" },
    MOST_SHORTED_STOCKS: { id: "most_shorted_stocks", name: "Most Shorted Stocks" },
    UNDERVALUED_LARGE_CAPS: { id: "undervalued_large_caps", name: "Undervalued Large Caps" },
    AGGRESSIVE_SMALL_CAPS: { id: "aggressive_small_caps", name: "Aggressive Small Caps" },
    GROWTH_TECHNOLOGY_STOCKS: { id: "growth_technology_stocks", name: "Growth Technology Stocks" },
    SMALL_CAP_GAINERS: { id: "small_cap_gainers", name: "Small Cap Gainers" },
    PORTFOLIO_ANCHORS: { id: "portfolio_anchors", name: "Portfolio Anchors" },
    CONSERVATIVE_FOREIGN_FUNDS: { id: "conservative_foreign_funds", name: "Conservative Foreign Funds" },
};
export class StockSymbolScannerService {
    /**
 * Fetches all stock symbols for the given region across all categories in `SCR_IDS`.
 * @param region - The region to fetch stocks for.
 * @returns A list of stock objects with `symbol`, `categoryId`, and `categoryName`.
 */
    async fetchStocksAll() {
        const allStocks = [];
        for (const key in SCR_IDS) {
            const scrId = SCR_IDS[key];
            try {
                logger.info(`Fetching stocks for ${scrId.name} (${scrId.id})`);
                const symbols = await this.fetchStocks(scrId);
                for (const symbol of symbols) {
                    allStocks.push({ symbol, categoryId: scrId.id, categoryName: scrId.name });
                }
            }
            catch (error) {
                logger.error(`Error fetching stocks for ${scrId.name} (${scrId.id}): ${error}`);
            }
        }
        return allStocks;
    }
    async fetchStocks(scrId) {
        try {
            const result = await yahooFinance.screener({
                scrIds: scrId.id,
                count: 100,
            }, {
                validateResult: false,
            });
            if (result.quotes && Array.isArray(result.quotes)) {
                return result.quotes.map((quote) => quote.symbol);
            }
            else {
                logger.warn(`No stock data found for ${scrId.name} (${scrId.id})`);
                return [];
            }
        }
        catch (error) {
            logger.error(`Error while fetching stocks for ${scrId.name} (${scrId.id}):`, error);
            return [];
        }
    }
}
