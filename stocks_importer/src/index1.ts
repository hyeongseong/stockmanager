import yahooFinance from 'yahoo-finance2';


async function getStockData(symbol: string): Promise<void> {
    try {
        const quote = await yahooFinance.quote(symbol);
        console.log(quote);
    } catch (error) {
        console.error('Error fetching Yahoo Finance data:', error);
    }
}


getStockData('ART.AX'); 