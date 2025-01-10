import config from 'config';
import dateformat from 'dateformat';
import * as fs from 'fs';
import * as path from 'path';
import * as winston from 'winston';
import 'winston-daily-rotate-file';
/**
 * LoggerService
 * Provides an extensible and configurable logging service using Winston.
 */
class LoggerService {
    /**
     * Initialize the logger with default configurations.
     */
    static initialize() {
        // Set log level from configuration, if available
        if (config.has('logger.debug')) {
            LoggerService.logLevel = config.get('logger.debug');
        }
        // Ensure the log folder exists
        if (!fs.existsSync(LoggerService.LOG_FOLDER)) {
            fs.mkdirSync(LoggerService.LOG_FOLDER);
        }
        // Create logger with transports
        LoggerService.logger = winston.createLogger({
            transports: [
                new winston.transports.Console({
                    level: 'silly',
                    format: winston.format.combine(winston.format.timestamp({ format: LoggerService.timestamp }), winston.format.printf(LoggerService.formatter)),
                }),
                new winston.transports.File({
                    level: 'silly',
                    filename: path.join(LoggerService.LOG_FOLDER, `${config.get('logger.name')}.log`),
                    maxsize: LoggerService.ONE_MB,
                    maxFiles: 5,
                    tailable: true,
                    format: winston.format.combine(winston.format.timestamp({ format: LoggerService.timestamp }), winston.format.printf(LoggerService.formatter)),
                }),
                new winston.transports.DailyRotateFile({
                    level: 'info',
                    filename: path.join(LoggerService.LOG_FOLDER, `${config.get('logger.name')}-%DATE%.log`),
                    datePattern: 'YYYY-MM-DD',
                    format: winston.format.combine(winston.format.timestamp({ format: LoggerService.timestamp }), winston.format.printf(LoggerService.formatter)),
                }),
            ],
        });
        LoggerService.logger.info(`Logger initialized with debug level: ${LoggerService.logLevel}`);
    }
    /**
     * Returns the current timestamp in a formatted string.
     */
    static timestamp() {
        return dateformat(new Date(), 'mmm dd HH:MM:ss');
    }
    /**
     * Formats log messages.
     */
    static formatter(info) {
        const levelLabels = {
            error: '(E)',
            warn: '(W)',
            info: '(I)',
            verbose: '(V)',
            debug: '(D)',
            silly: '(D)',
        };
        return `${info.timestamp || ''} ${levelLabels[info.level] || '(?)'} ${info.message || ''} ${info.meta && Object.keys(info.meta).length ? JSON.stringify(info.meta) : ''}`;
    }
    /**
     * Logs debug messages with a specified debug level.
     */
    static debug(level, ...messages) {
        if (level <= LoggerService.logLevel) {
            LoggerService.logger.debug(messages.join(' '));
            return true;
        }
        return false;
    }
    /**
     * Sets the log filename and reinitializes transports.
     */
    static setFilename(filename) {
        LoggerService.logger.clear();
        LoggerService.logger.add(new winston.transports.Console({
            level: 'silly',
            format: winston.format.combine(winston.format.timestamp({ format: LoggerService.timestamp }), winston.format.printf(LoggerService.formatter)),
        }));
        LoggerService.logger.add(new winston.transports.File({
            level: 'silly',
            filename: path.join(LoggerService.LOG_FOLDER, `${filename}.log`),
            maxsize: LoggerService.ONE_MB,
            maxFiles: 5,
            tailable: true,
            format: winston.format.combine(winston.format.timestamp({ format: LoggerService.timestamp }), winston.format.printf(LoggerService.formatter)),
        }));
        LoggerService.logger.add(new winston.transports.DailyRotateFile({
            level: 'info',
            filename: path.join(LoggerService.LOG_FOLDER, `${filename}-%DATE%.log`),
            datePattern: 'YYYY-MM-DD',
            format: winston.format.combine(winston.format.timestamp({ format: LoggerService.timestamp }), winston.format.printf(LoggerService.formatter)),
        }));
        LoggerService.logger.info(`Logger filename updated to: ${filename}`);
    }
    // Logging methods for different levels
    static error(...messages) {
        LoggerService.logger.error(messages.join(' '));
    }
    static warn(...messages) {
        LoggerService.logger.warn(messages.join(' '));
    }
    static info(...messages) {
        LoggerService.logger.info(messages.join(' '));
    }
    static verbose(...messages) {
        LoggerService.logger.verbose(messages.join(' '));
    }
}
LoggerService.ONE_MB = 1 * 1024 * 1024; // Maximum file size for logs
LoggerService.LOG_FOLDER = 'log'; // Default log folder
LoggerService.logLevel = 1; // Default debug level
// Static initialization block to automatically initialize the logger
(() => {
    LoggerService.initialize();
})();
export default LoggerService;
