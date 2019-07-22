import cluster from 'cluster';
import tradeBucketService from '../service/tradeBucketService';
import volumeService from '../service/volumeService';
import fftService from '../service/fftService';
import bitmexInstrumentService from '../service/bitmexInstrumentService';
import deribitInstrumentService from '../service/deribitInstrumentService';

if (cluster.isMaster) {
    cluster.fork();
    cluster.on('exit', function (worker, code, signal) {
        cluster.fork();
    });
}

if (cluster.isWorker) {
    //trades bucket
    setTimeout(tradeBucketService.getLastTimestamp, 0, '1m', (binSize, timestamp) => {
        tradeBucketService.downloadTradeBucketed(binSize, timestamp);
    });
    setTimeout(tradeBucketService.getLastTimestamp, 10000, '5m', (binSize, timestamp) => {
        tradeBucketService.downloadTradeBucketed(binSize, timestamp);
    });
    setTimeout(tradeBucketService.getLastTimestamp, 20000, '1h', (binSize, timestamp) => {
        tradeBucketService.downloadTradeBucketed(binSize, timestamp);
    });

    //volume
    volumeService.startRead([
        'trade:XBTUSD',
    ]);
    setTimeout(volumeService.saveTradesBuffer, 10000);
    setTimeout(volumeService.calculateVolume, 20000);

    //fft
    setTimeout(fftService.getLastTimestamp, 5000, '5m', (binSize, timestamp) => {
        fftService.calculateFFT(binSize, timestamp);
    });
    setTimeout(fftService.getLastTimestamp, 15000, '1h', (binSize, timestamp) => {
        fftService.calculateFFT(binSize, timestamp);
    });

    //bitmex instruments
    bitmexInstrumentService.downloadInstrument();

    //deribit instruments
    deribitInstrumentService.downloadDeribitInstruments();
}
