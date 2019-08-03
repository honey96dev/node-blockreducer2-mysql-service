import cluster from 'cluster';
import bitmexTradeBucketService from '../service/bitmexTradeBucketService';
import bitfinexCandleTradeService from '../service/bitfinexCandleTradeService';
import bitfinexCandleTradeService1 from '../service/bitfinexCandleTradeService1';
import bitmexVolumeService from '../service/bitmexVolumeService';
import fftService from '../service/fftService';
import fftService1 from '../service/fftService1';
import bitmexInstrumentService from '../service/bitmexInstrumentService';
import deribitInstrumentService from '../service/deribitInstrumentService';
import id0Service from '../service/id0Service';

if (cluster.isMaster) {
    cluster.fork();
    cluster.on('exit', function (worker, code, signal) {
        cluster.fork();
    });
}

if (cluster.isWorker) {
    //trades bucket
    setTimeout(bitmexTradeBucketService.getLastTimestamp, 0, 'XBTUSD', '1m', (symbol, binSize, timestamp) => {
        bitmexTradeBucketService.downloadTradeBucketed(symbol, binSize, timestamp);
    });
    setTimeout(bitmexTradeBucketService.getLastTimestamp, 3000, 'XBTUSD', '5m', (symbol, binSize, timestamp) => {
        bitmexTradeBucketService.downloadTradeBucketed(symbol, binSize, timestamp);
    });
    setTimeout(bitmexTradeBucketService.getLastTimestamp, 6000, 'XBTUSD', '1h', (symbol, binSize, timestamp) => {
        bitmexTradeBucketService.downloadTradeBucketed(symbol, binSize, timestamp);
    });
    setTimeout(bitfinexCandleTradeService.getLastTimestamp, 0, 'tETHUSD', '1m', (symbol, binSize, timestamp) => {
        bitfinexCandleTradeService.downloadCandleTrade(symbol, binSize, timestamp);
    });
    setTimeout(bitfinexCandleTradeService.getLastTimestamp, 3500, 'tETHUSD', '5m', (symbol, binSize, timestamp) => {
        bitfinexCandleTradeService.downloadCandleTrade(symbol, binSize, timestamp);
    });
    setTimeout(bitfinexCandleTradeService.getLastTimestamp, 6500, 'tETHUSD', '1h', (symbol, binSize, timestamp) => {
        bitfinexCandleTradeService.downloadCandleTrade(symbol, binSize, timestamp);
    });
    setTimeout(bitfinexCandleTradeService1.getLastTimestamp, 0, 'tBCHUSD', '1m', (symbol, binSize, timestamp) => {
        bitfinexCandleTradeService1.downloadCandleTrade(symbol, binSize, timestamp);
    });
    // setTimeout(bitfinexCandleTradeService1.getLastTimestamp, 0, 'tBCHUSD', '5m', (symbol, binSize, timestamp) => {
    //     bitfinexCandleTradeService1.downloadCandleTrade(symbol, binSize, timestamp);
    // });
    // setTimeout(bitfinexCandleTradeService1.getLastTimestamp, 6500, 'tBCHUSD', '1h', (symbol, binSize, timestamp) => {
    //     bitfinexCandleTradeService1.downloadCandleTrade(symbol, binSize, timestamp);
    // });


    //volume
    bitmexVolumeService.startRead([
        'trade:XBTUSD',
    ]);
    setTimeout(bitmexVolumeService.saveTradesBuffer, 10000);
    setTimeout(bitmexVolumeService.calculateVolume, 20000);

    id0Service.startCalculation();

    //fft
    setTimeout(fftService.getLastTimestamp, 5000, 'XBTUSD', '5m', (symbol, binSize, timestamp) => {
        fftService.calculateFFT(symbol, binSize, timestamp);
    });
    setTimeout(fftService.getLastTimestamp, 15000, 'XBTUSD', '1h', (symbol, binSize, timestamp) => {
        fftService.calculateFFT(symbol, binSize, timestamp);
    });
    setTimeout(fftService.getLastTimestamp, 5000, 'tETHUSD', '5m', (symbol, binSize, timestamp) => {
        fftService.calculateFFT(symbol, binSize, timestamp);
    });
    setTimeout(fftService.getLastTimestamp, 15000, 'tETHUSD', '1h', (symbol, binSize, timestamp) => {
        fftService.calculateFFT(symbol, binSize, timestamp);
    });

    //fft-hist
    setTimeout(fftService1.getLastTimestamp, 5000, 'tBCHUSD', '5m', (symbol, binSize, timestamp) => {
        fftService1.calculateFFT(symbol, binSize, timestamp);
    });
    setTimeout(fftService1.getLastTimestamp, 15000, 'tBCHUSD', '1h', (symbol, binSize, timestamp) => {
        fftService1.calculateFFT(symbol, binSize, timestamp);
    });

    //bitmex instruments
    bitmexInstrumentService.downloadInstrument();

    //deribit instruments
    deribitInstrumentService.downloadDeribitInstruments();
}
