import request from 'request';
import {dbTblName, deribit} from '../core/config';
import {sprintf} from 'sprintf-js';
import dbConn from '../core/dbConn';

let service = {
    delayInstruments: 30000,
    delayTicker: 0,

    timeoutInstrumentsId: undefined,
    timeoutTickersId: undefined,

    instrumentsBuffer: [],
    detailRowsBuffer: [],
    socket: undefined,
};

service.downloadDeribitInstruments = () => {
    if (service.timeoutInstrumentsId) {
        clearTimeout(service.timeoutInstrumentsId);
        service.timeoutInstrumentsId = undefined;
    }

    const baseUrl = deribit.testnet ? deribit.baseUrlTestnet : deribit.baseUrlRealnet;
    let url = sprintf("%s%s?currency=BTC&kind=option&expired=false", baseUrl, deribit.pathInstruments);
    console.log('downloadDeribitInstruments', url);
    request(url, null, (error, response, body) => {
        if (error) {
            console.warn(error);
            service.timeoutInstrumentsId = setTimeout(service.downloadDeribitInstruments, service.delayInstruments);
            return;
        }
        body = JSON.parse(body);
        const instruments = body.result;
        console.log('instruments', new Date(), instruments.length);

        for (let instrument of instruments) {
            service.instrumentsBuffer.push(instrument);
        }
        service.detailRowsBuffer = [];
        service.downloadTicker();
    });
};

service.downloadTicker = () => {
    if (service.timeoutTickersId) {
        clearTimeout(service.timeoutTickersId);
        service.timeoutTickersId = undefined;
    }
    let instrument = service.instrumentsBuffer.shift();
    const baseUrl = deribit.testnet ? deribit.baseUrlTestnet : deribit.baseUrlRealnet;
    let url = sprintf("%s%s?instrument_name=%s", baseUrl, deribit.pathTicker, instrument.instrument_name);
    // console.log(url);
    request(url, undefined, (error, response, body) => {
        if (error) {
            console.warn(error);
            if (service.instrumentsBuffer.length > 0) {
                service.timeoutTickersId = setTimeout(service.downloadTicker, service.delayTicker);
            } else {
                service.calculateChartData();
            }
            return;
        }
        // console.log(body);
        body = JSON.parse(body);
        body = body.result;
        const expiration_timestamp = new Date(instrument.expiration_timestamp);
        const creation_timestamp = new Date(instrument.creation_timestamp);
        const type_symbol = instrument.instrument_name.substr(instrument.instrument_name.length - 1, 1);
        const strike = Math.round(parseFloat(instrument.strike) * 1000);
        const option_symbol = sprintf("GS%02d%02d%02d%s%08d", expiration_timestamp.getFullYear() % 100, expiration_timestamp.getMonth() + 1, expiration_timestamp.getDate(), type_symbol, strike);
        const type = type_symbol == 'C' ? 'Call' : 'Put';
        service.detailRowsBuffer.push([
            typeof instrument.instrument_name === 'undefined' ? '' : instrument.instrument_name,
            typeof body.underlying_price === 'undefined' ? 0 : body.underlying_price,
            option_symbol,
            type,
            expiration_timestamp.toISOString(),
            creation_timestamp.toISOString(),
            typeof instrument.strike === 'undefined' ? 0 : instrument.strike,
            typeof body.last_price === 'undefined' ? 0 : body.last_price,
            typeof body.best_bid_price === 'undefined' ? 0 : body.best_bid_price,
            typeof body.best_ask_price === 'undefined' ? 0 : body.best_ask_price,
            typeof body.stats.volume === 'undefined' ? 0 : body.stats.volume,
            typeof body.open_interest === 'undefined' ? 0 : body.open_interest,
            typeof body.mark_iv === 'undefined' ? 0 : body.mark_iv,
            typeof body.bid_iv === 'undefined' ? 0 : body.bid_iv,
            typeof body.iv_ask === 'undefined' ? 0 : body.iv_ask,
            typeof body.greeks.delta === 'undefined' ? 0 : body.greeks.delta,
            typeof body.greeks.gamma === 'undefined' ? 0 : body.greeks.gamma,
            typeof body.greeks.theta === 'undefined' ? 0 : body.greeks.theta,
            typeof body.greeks.vega === 'undefined' ? 0 : body.greeks.vega
        ]);
        if (service.instrumentsBuffer.length > 0) {
            service.timeoutTickersId = setTimeout(service.downloadTicker, service.delayTicker);
        } else {
            service.calculateChartData();
        }
    });
};

service.calculateChartData = () => {
    console.log('calculateChartData', new Date());

    let sql = sprintf("DELETE FROM `%s`;", dbTblName.deribitInstruments);
    dbConn.query(sql, undefined, (error, results, fields) => {
        if (error) {
            console.warn(error);
            service.timeoutInstrumentsId = setTimeout(service.downloadDeribitInstruments, service.delayInstruments);
            return;
        }
        sql = sprintf("INSERT INTO `%s`(`instrument_name`, `underlying_price`, `option_symbol`, `type`, `expiration_timestamp`, `creation_timestamp`, `strike`, `last_price`, `best_bid_price`, `best_ask_price`, `volume`, `open_interest`, `mark_iv`, `bid_iv`, `iv_ask`, `delta`, `gamma`, `theta`, `vega`) VALUES ?;", dbTblName.deribitInstruments);
        console.log('downloadDeribitInstruments', service.detailRowsBuffer.length, sql);
        dbConn.query(sql, [service.detailRowsBuffer], (error, results, fields) => {
            if (error) {
                console.warn(error);
                service.timeoutInstrumentsId = setTimeout(service.downloadDeribitInstruments, service.delayInstruments);
                return;
            }
            sql = sprintf("DELETE FROM `deribit_instruments2`;");
            dbConn.query(sql, undefined, (error, results, fields) => {
                if (error) {
                    console.warn(error);
                    service.timeoutInstrumentsId = setTimeout(service.downloadDeribitInstruments, service.delayInstruments);
                    return;
                }
                sql = sprintf("INSERT INTO `%s`(`instrument_name`, `underlying_price`, `option_symbol`, `type`, `expiration_timestamp`, `creation_timestamp`, `strike`, `last_price`, `best_bid_price`, `best_ask_price`, `volume`, `open_interest`, `mark_iv`, `bid_iv`, `iv_ask`, `delta`, `gamma`, `theta`, `vega`) (SELECT `instrument_name`, `underlying_price`, `option_symbol`, `type`, `expiration_timestamp`, `creation_timestamp`, `strike`, `last_price`, `best_bid_price`, `best_ask_price`, `volume`, `open_interest`, `mark_iv`, `bid_iv`, `iv_ask`, `delta`, `gamma`, `theta`, `vega` FROM `deribit_instruments`);", dbTblName.deribitInstruments2);

                dbConn.query(sql, undefined, (error, results, fields) => {
                    service.timeoutInstrumentsId = setTimeout(service.downloadDeribitInstruments, service.delayInstruments);
                });
            });
        });
    });
};

module.exports = service;
