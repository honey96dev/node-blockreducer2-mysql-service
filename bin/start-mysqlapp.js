import cluster from 'cluster';
import tradeBucketService from '../service/tradeBucketService';
import volumeService from '../service/volumeService';
import fftService from '../service/fftService';
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
    setTimeout(tradeBucketService.getLastTimestamp, 0, 'XBTUSD', '1m', (symbol, binSize, timestamp) => {
        tradeBucketService.downloadTradeBucketed(symbol, binSize, timestamp);
    });
    setTimeout(tradeBucketService.getLastTimestamp, 10000, 'XBTUSD', '5m', (symbol, binSize, timestamp) => {
        tradeBucketService.downloadTradeBucketed(symbol, binSize, timestamp);
    });
    setTimeout(tradeBucketService.getLastTimestamp, 20000, 'XBTUSD', '1h', (symbol, binSize, timestamp) => {
        tradeBucketService.downloadTradeBucketed(symbol, binSize, timestamp);
    });

    //volume
    volumeService.startRead([
        'trade:XBTUSD',
    ]);
    setTimeout(volumeService.saveTradesBuffer, 10000);
    setTimeout(volumeService.calculateVolume, 20000);

    id0Service.startCalculation();

    //fft
    setTimeout(fftService.getLastTimestamp, 5000, 'XBTUSD', '5m', (symbol, binSize, timestamp) => {
        fftService.calculateFFT(symbol, binSize, timestamp);
    });
    setTimeout(fftService.getLastTimestamp, 15000, 'XBTUSD', '1h', (symbol, binSize, timestamp) => {
        fftService.calculateFFT(symbol, binSize, timestamp);
    });

    //bitmex instruments
    bitmexInstrumentService.downloadInstrument();

    //deribit instruments
    deribitInstrumentService.downloadDeribitInstruments();
}
