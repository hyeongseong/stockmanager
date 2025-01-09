import axios from 'axios';

async function searchYahooFinance(query: string): Promise<void> {
    const url = 'https://query1.finance.yahoo.com/v1/finance/search';

    try {
        const response = await axios.get(url, {
            params: {
                q: query, // 검색어
            },
            headers: {
                'Content-Type': 'application/json',
            },
        });

        // 전체 데이터 출력
        console.log('Full Search Result:', JSON.stringify(response.data, null, 2));

        // 검색 결과의 첫 번째 항목만 출력
        if (response.data.quotes && response.data.quotes.length > 0) {
            console.log('First Search Result:', response.data.quotes[0]);
        } else {
            console.log('No results found for query:', query);
        }
    } catch (error) {
        console.error('Error searching Yahoo Finance:', error);
    }
}

// 호출 예제
searchYahooFinance('AAPL'); // Apple 검색
