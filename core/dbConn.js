import mysql from 'mysql2';
import config from './config';

module.exports = mysql.createPool(config.mysql);
