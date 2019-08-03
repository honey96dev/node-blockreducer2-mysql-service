import {dbTblName} from '../core/config';
import dbConn from '../core/dbConn';
import Fili from 'fili';
import {sprintf} from 'sprintf-js';

let service = {
    timeoutId: {},
    timeoutDelay: 100,
};

service.calculateFFT = (symbol, binSize, timestamp) => {
    if (timestamp.length === 0) {
        if (binSize === '5m') {
            if (symbol === 'XBTUSD') {
                timestamp = '2015-09-25T12:05:00.000Z';
            } else {
                timestamp = 0;
            }
        } else if (binSize === '1h') {
            if (symbol) {
                timestamp = '2015-09-25T13:00:00.000Z';
            } else {
                timestamp = 0;
            }
        }
    }
    const timeoutIdKey = symbol + ':' + binSize;
    if (service.timeoutId[timeoutIdKey]) {
        clearTimeout(service.timeoutId[timeoutIdKey]);
    }
    let timeStep;
    if (binSize === '5m') {
        timeStep = 5 * 60 * 1000;
    } else if (binSize === '1h') {
        timeStep = 60 * 60 * 1000;
    }
    timestamp = new Date(new Date(timestamp).getTime() - timeStep * 500).toISOString();
    let sql = sprintf("SELECT * FROM `%s_%s_%s` WHERE `timestamp` > '%s' ORDER BY `timestamp` LIMIT 1000;", dbTblName.tradeBucketed, symbol, binSize, timestamp);
    // console.log('calculateFFT', symbol, binSize, timestamp, sql);
    dbConn.query(sql, null, (error, results, fields) => {
        if (error) {
            console.log(error);
        } else {
            let calced = [];
            let timestamps = [];
            let open = [];
            let high = [];
            let low = [];
            let close = [];
            let maxChange = [];
            let lowPass = [];
            let highPass = [];
            let maxChange1;
            if (results != null && results.length > 0) {
                for (let item of results) {
                    timestamps.push(item.timestamp);
                    open.push(item.open);
                    high.push(item.high);
                    low.push(item.low);
                    close.push(item.close);
                    maxChange1 = ((parseFloat(item.high) - parseFloat(item.low)) / parseFloat(item.close));
                    if (isNaN(maxChange1)) {
                        maxChange1 = 0
                    }
                    maxChange.push(maxChange1);

                    timestamp = item.timestamp;
                }
                const resultLast = results.length - 1;
                let lastTime = new Date(results[resultLast].timestamp);
                let timeStep = 0;
                if (binSize === '1m') {
                    timeStep = 60000;
                } else if (binSize === '5m') {
                    timeStep = 300000;
                } else if (binSize === '1h') {
                    timeStep = 3600000;
                }
                for (let i = 0; i < 100; i++) {
                    lastTime = new Date(lastTime.getTime() + timeStep);
                    timestamps.push(lastTime.toISOString());
                    open.push(results[resultLast].open);
                    high.push(results[resultLast].high);
                    low.push(results[resultLast].low);
                    close.push(results[resultLast].close);
                    maxChange1 = ((parseFloat(results[resultLast].high) - parseFloat(results[resultLast].low)) / parseFloat(results[resultLast].close));
                    if (isNaN(maxChange1)) {
                        maxChange1 = 0
                    }
                    maxChange.push(maxChange1);
                }

                const iirCalculator = new Fili.CalcCascades();

                const lowpassFilterCoeffs = iirCalculator.lowpass({
                    order: 3, // cascade 3 biquad filters (max: 12)
                    characteristic: 'butterworth',
                    Fs: 800, // sampling frequency
                    Fc: 80, // cutoff frequency / center frequency for bandpass, bandstop, peak
                    BW: 1, // bandwidth only for bandstop and bandpass filters - optional
                    gain: 0, // gain for peak, lowshelf and highshelf
                    preGain: false // adds one constant multiplication for highpass and lowpass
                    // k = (1 + cos(omega)) * 0.5 / k = 1 with preGain == false
                });

                const iirLowpassFilter = new Fili.IirFilter(lowpassFilterCoeffs);

                lowPass = iirLowpassFilter.multiStep(maxChange);

                const highpassFilterCoeffs = iirCalculator.highpass({
                    order: 3, // cascade 3 biquad filters (max: 12)
                    characteristic: 'butterworth',
                    Fs: 800, // sampling frequency
                    Fc: 80, // cutoff frequency / center frequency for bandpass, bandstop, peak
                    BW: 1, // bandwidth only for bandstop and bandpass filters - optional
                    gain: 0, // gain for peak, lowshelf and highshelf
                    preGain: false // adds one constant multiplication for highpass and lowpass
                    // k = (1 + cos(omega)) * 0.5 / k = 1 with preGain == false
                });

                const iirHighpassFilter = new Fili.IirFilter(highpassFilterCoeffs);
                highPass = iirHighpassFilter.multiStep(maxChange);
            }
            if (timestamps.length === 0) {
                return;
            }
            for (let i = 0; i < timestamps.length - 100; i++) {
                calced.push([
                    timestamps[i],
                    open[i],
                    high[i],
                    low[i],
                    close[i],
                    lowPass[i],
                    highPass[i]
                ])
            }

            let sql = sprintf("INSERT INTO `%s_%s_%s`(`timestamp`, `open`, `high`, `low`, `close`, `lowPass`, `highPass`) VALUES ? ON DUPLICATE KEY UPDATE `open` = VALUES(`open`), `high` = VALUES(`high`), `low` = VALUES(`low`), `close` = VALUES(`close`), `lowPass` = VALUES(`lowPass`), `highPass` = VALUES(`highPass`);", dbTblName.fft, symbol, binSize);
            let buffer = [];
            for (let item of calced) {
                buffer.push(item);
                if (buffer.length > 512) {
                    dbConn.query(sql, [buffer], (error, results, fields) => {
                        if (error) {
                            console.log(error);
                            // dbConn = null;
                        } else {
                        }

                    });
                    buffer = [];
                }
            }
            if (buffer.length > 0) {
                dbConn.query(sql, [buffer], (error, results, fields) => {
                    if (error) {
                        console.log(error);
                        // dbConn = null;
                    } else {

                    }
                });
            }
        }
        service.timeoutId[timeoutIdKey] = setTimeout(service.calculateFFT, service.timeoutDelay, symbol, binSize, timestamp);
    });
};

service.getLastTimestamp = (symbol, binSize, cb) => {
    let sql = sprintf("SELECT `timestamp` FROM `%s_%s_%s` ORDER BY `timestamp` DESC LIMIT 1;", dbTblName.fft, symbol, binSize);
    dbConn.query(sql, null, (error, rows, fields) => {
        if (error || rows.length === 0) {
            cb(symbol, binSize, '');
        } else {
            cb(symbol, binSize, rows[0]['timestamp']);
        }
    });
};

module.exports = service;
