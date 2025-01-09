import axios from 'axios';

async function fetchStockChartData(symbol: string): Promise<void> {
    const url = `https://query1.finance.yahoo.com/v8/finance/chart/${symbol}`;
    
    try {
        const response = await axios.get(url, {
            params: {
                region: 'US',
                interval: '1d',
                range: '1mo',
            },
            headers: {
                'Content-Type': 'application/json',
            },
        });

        const data = response.data;
        console.log('Full Stock Chart Data:', JSON.stringify(data, null, 2)); // 전체 데이터를 보기 쉽게 출력

        // `result` 안의 첫 번째 객체를 출력
        if (data.chart && data.chart.result) {
            console.log('Parsed Stock Chart Data:', JSON.stringify(data.chart.result[0], null, 2));
        }
    } catch (error) {
        console.error('Error fetching stock chart data:', error);
    }
}

// 호출 예제
fetchStockChartData('AAPL');
