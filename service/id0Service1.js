import {dbTblName} from '../core/config';
import dbConn from '../core/dbConn';
import fftJs from 'fft-js';
import {sprintf} from 'sprintf-js';

let service = {
    timeoutId: {},
    timeoutDelay: 0,
};

service.calculateId0 = (symbol, binSize, timestamp) => {
    const timeoutIdKey = symbol + ':' + binSize;
    if (typeof service.timeoutId[timeoutIdKey] !== 'undefined') {
        clearTimeout(service.timeoutId[timeoutIdKey]);
    }
    // let sql = sprintf("SELECT `timestamp` FROM `id0_%s_` ORDER BY `timestamp` DESC LIMIT 1;", interval);
    // dbConn.query(sql, undefined, (error, results, fields) => {
    //     if (error) {
    //         console.log(error);
    //         intervalId[interval] = setTimeout(calculate, delay, interval, timestamp);
    //         return;
    //     }
    let id0LastTimestamp = '';
    if (timestamp.length > 0) {
        // id0LastTimestamp = results[0].timestamp;
        let timeStep = 0;
        if (binSize === '1m') {
            timeStep = 60000;
        } else if (binSize === '5m') {
            timeStep = 300000;
        } else if (binSize === '1h') {
            timeStep = 3600000;
        }
        id0LastTimestamp = new Date(new Date(timestamp).getTime() + timeStep).toISOString();
    } else {
        if (binSize === '1m') {
            if (symbol === 'XBTUSD') {
                id0LastTimestamp = '2019-04-25T00:01:00.000Z';
            } else if (symbol === 'tETHUSD') {
                id0LastTimestamp = '2016-03-29T09:31:00.000Z';
            } else if (symbol === 'tBCHUSD') {
                id0LastTimestamp = '2017-08-02T08:53:00.000Z';
            } else if (symbol === 'tEOSUSD') {
                id0LastTimestamp = '2017-07-01T16:59:00.000Z';
            } else if (symbol === 'tLTCUSD') {
                id0LastTimestamp = '2013-05-19T15:23:00.000Z';
            } else if (symbol === 'tBSVUSD') {
                id0LastTimestamp = '2018-11-13T10:39:00.000Z';
            }
        } else if (binSize === '5m') {
            if (symbol === 'XBTUSD') {
                id0LastTimestamp = '2015-09-25T12:05:00.000Z';
            } else if (symbol === 'tETHUSD') {
                id0LastTimestamp = '2016-03-09T16:00:00.000Z';
            } else if (symbol === 'tBCHUSD') {
                id0LastTimestamp = '2017-08-02T08:50:00.000Z';
            } else if (symbol === 'tEOSUSD') {
                id0LastTimestamp = '2017-07-01T16:55:00.000Z';
            } else if (symbol === 'tLTCUSD') {
                id0LastTimestamp = '2013-05-19T15:20:00.000Z';
            } else if (symbol === 'tBSVUSD') {
                id0LastTimestamp = '2018-11-13T10:35:00.000Z';
            }
        } else if (binSize === '1h') {
            if (symbol === 'XBTUSD') {
                id0LastTimestamp = '2015-11-19T20:00:00.000Z';
            } else if (symbol === 'tETHUSD') {
                id0LastTimestamp = '2016-03-09T16:00:00.000Z';
            } else if (symbol === 'tBCHUSD') {
                id0LastTimestamp = '2017-08-02T08:00:00.000Z';
            } else if (symbol === 'tEOSUSD') {
                id0LastTimestamp = '2017-07-01T16:00:00.000Z';
            } else if (symbol === 'tLTCUSD') {
                id0LastTimestamp = '2013-05-19T15:00:00.000Z';
            } else if (symbol === 'tBSVUSD') {
                id0LastTimestamp = '2018-11-13T10:00:00.000Z';
            }
        }
    }

    let sql = sprintf("SELECT * FROM (SELECT * FROM `%s_%s_%s` WHERE `timestamp` <= '%s' ORDER BY `timestamp` DESC LIMIT 2000) `tmp` ORDER BY `timestamp` ASC;", dbTblName.tradeBucketed, symbol, binSize, id0LastTimestamp);
    // console.log(interval, id0LastTimestamp);
    dbConn.query(sql, undefined, (error, results, fields) => {
        if (error) {
            console.error(error);
            service.timeoutId[timeoutIdKey] = setTimeout(service.calculateId0, service.timeoutDelay, symbol, binSize, id0LastTimestamp);
            return;
        }
        // console.log(JSON.stringify(results));
        let resultCnt = results.length;
        if (results.length < 2048 && results.length > 0) {
            const cnt = resultCnt;
            let lastTime = new Date(results[cnt - 1].timestamp);
            let timeStep = 0;
            if (binSize === '1m') {
                timeStep = 60000;
            } else if (binSize === '5m') {
                timeStep = 300000;
            } else if (binSize === '1h') {
                timeStep = 3600000;
            }
            const last = results[cnt - 1];
            for (let i = cnt; i < 2048; i++) {
                // results.push(last);
                lastTime = new Date(lastTime.getTime() + timeStep);
                results.push({
                    // timestamp: last.timestamp,
                    timestamp: lastTime.toISOString(),
                    symbol: last.symbol,
                    open: last.open,
                    high: last.high,
                    low: last.low,
                    close: last.close,
                    volume: last.volume,
                    lowPass: last.lowPass,
                    highPass: last.highPass,
                });
            }
        }
        // let dates = [];
        let opens = [];
        for (let item of results) {
            // dates.push(item.date);
            opens.push(item.open);
        }
        let fft = fftJs.fft(opens);
        const cnts = [3, 6, 9, 100];
        let buffer;
        let iffts = new Map();
        for (let cnt of cnts) {
            let i;
            const cnt2 = 2048 - cnt;
            let ifft;
            buffer = [];
            for (i = 0; i < cnt; i++) {
                buffer.push(fft[i]);
            }
            for (i = cnt; i < cnt2; i++) {
                buffer.push([0, 0]);
            }
            for (i = cnt2; i < 2048; i++) {
                buffer.push(fft[i]);
            }
            ifft = fftJs.ifft(buffer);
            iffts.set('ifft' + cnt, ifft);
        }

        let ifft3 = iffts.get('ifft3');
        let ifft6 = iffts.get('ifft6');
        let ifft9 = iffts.get('ifft9');
        let ifft100 = iffts.get('ifft100');

        const finalIdx = resultCnt - 1;
        const row =[
            results[finalIdx].timestamp,
            results[finalIdx].open,
            results[finalIdx].high,
            results[finalIdx].low,
            results[finalIdx].close,
            ifft3[finalIdx][0],
            ifft3[finalIdx][1],
            ifft6[finalIdx][0],
            ifft6[finalIdx][1],
            ifft9[finalIdx][0],
            ifft9[finalIdx][1],
            ifft100[finalIdx][0],
            ifft100[finalIdx][1],
        ];
        // console.log(ifft100);
        // let sql = sprintf("INSERT INTO `id0_%s` (`timestamp`, `open`, `high`, `low`, `close`, `num_3`, `num_3i`, `num_6`, `num_6i`, `num_9`, `num_9i`, `num_100`, `num_100i`) VALUES ? ON DUPLICATE KEY UPDATE `timestamp` = VALUES(`timestamp`), `open` = VALUES(`open`), `high` = VALUES(`high`), `low` = VALUES(`low`), `close` = VALUES(`close`), `num_3` = VALUES(`num_3`), `num_3i` = VALUES(`num_3i`), `num_6` = VALUES(`num_6`), `num_6i` = VALUES(`num_6i`), `num_9` = VALUES(`num_9`), `num_9i` = VALUES(`num_9i`), `num_100` = VALUES(`num_100`), `num_100i` = VALUES(`num_100i`);", interval);
        let sql = sprintf("INSERT INTO `%s_%s_%s` (`timestamp`, `open`, `high`, `low`, `close`, `num_3`, `num_3i`, `num_6`, `num_6i`, `num_9`, `num_9i`, `num_100`, `num_100i`) VALUES ('%s', '%f', '%f', '%f', '%f', '%f', '%f', '%f', '%f', '%f', '%f', '%f', '%f') ON DUPLICATE KEY UPDATE `open` = VALUES(`open`), `high` = VALUES(`high`), `low` = VALUES(`low`), `close` = VALUES(`close`), `num_3` = VALUES(`num_3`), `num_3i` = VALUES(`num_3i`), `num_6` = VALUES(`num_6`), `num_6i` = VALUES(`num_6i`), `num_9` = VALUES(`num_9`), `num_9i` = VALUES(`num_9i`), `num_100` = VALUES(`num_100`), `num_100i` = VALUES(`num_100i`);", dbTblName.id0, symbol, binSize, results[finalIdx].timestamp,
            !!results[finalIdx].open ? results[finalIdx].open : 0,
            !!results[finalIdx].high ? results[finalIdx].high : 0,
            !!results[finalIdx].low ? results[finalIdx].low : 0,
            !!results[finalIdx].close ? results[finalIdx].close : 0,
            ifft3[finalIdx][0],
            ifft3[finalIdx][1],
            ifft6[finalIdx][0],
            ifft6[finalIdx][1],
            ifft9[finalIdx][0],
            ifft9[finalIdx][1],
            ifft100[finalIdx][0],
            ifft100[finalIdx][1]);
        console.log(sql);
        dbConn.query(sql, null, (error, results2, fields) => {
            if (error) {
                console.error(error);
                service.timeoutId[timeoutIdKey] = setTimeout(service.calculateId0, service.timeoutDelay, symbol, binSize, timestamp);
                return;
            }
            // sql = sprintf("SELECT `timestamp` FROM `%s_%s` ORDER BY `timestamp` DESC LIMIT 1;", binSize);
            service.timeoutId[timeoutIdKey] = setTimeout(service.calculateId0, service.timeoutDelay, symbol, binSize, id0LastTimestamp);
            // dbConn.query(sql, null, (error, results5, fields) => {
            //     if (error) {
            //         console.error(error);
            //     } else {
            //         console.log(binSize, results5[0].timestamp, results[finalIdx].timestamp);
            //         if (results5 && results5.length > 0) {
            //             if (results5[0].timestamp < results[finalIdx].timestamp) {
            //
            //                 console.warn(new Date(), binSize, 'done');
            //                 return;
            //             }
            //         }
            //     }
            //     // console.log('interval-set', interval, new Date());
            // });
        });
    });
};

service.startCalculation = (symbol) => {
    let sql = sprintf("SELECT `timestamp` FROM `%s_%s_%s` ORDER BY `timestamp` DESC LIMIT 1;", dbTblName.id0, symbol, '1m');
    dbConn.query(sql, undefined, (error, results, fields) => {
        if (error || results.length == 0) {
            service.calculateId0(symbol, '1m', '');
        } else {
            service.calculateId0(symbol, '1m', results[0].timestamp);
        }
    });
    sql = sprintf("SELECT `timestamp` FROM `%s_%s_%s` ORDER BY `timestamp` DESC LIMIT 1;", dbTblName.id0, symbol, '5m');
    dbConn.query(sql, undefined, (error, results, fields) => {
        if (error || results.length == 0) {
            service.calculateId0(symbol, '5m', '');
        } else {
            service.calculateId0(symbol, '5m', results[0].timestamp);
        }
    });
    sql = sprintf("SELECT `timestamp` FROM `%s_%s_%s` ORDER BY `timestamp` DESC LIMIT 1;", dbTblName.id0, symbol, '1h');
    dbConn.query(sql, undefined, (error, results, fields) => {
        if (error || results.length == 0) {
            service.calculateId0(symbol, '1h', '');
        } else {
            service.calculateId0(symbol, '1h', results[0].timestamp);
        }
    });
};

module.exports = service;
