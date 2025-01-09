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
    private static readonly ONE_MB = 1 * 1024 * 1024; // Maximum file size for logs
    private static readonly LOG_FOLDER = 'log'; // Default log folder
    private static logLevel = 1; // Default debug level
    private static logger: winston.Logger;

    // Static initialization block to automatically initialize the logger
    static {
        LoggerService.initialize();
    }

    /**
     * Initialize the logger with default configurations.
     */
    private static initialize(): void {
        // Set log level from configuration, if available
        if (config.has('logger.debug')) {
            LoggerService.logLevel = config.get<number>('logger.debug');
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
                    format: winston.format.combine(
                        winston.format.timestamp({ format: LoggerService.timestamp }),
                        winston.format.printf(LoggerService.formatter)
                    ),
                }),
                new winston.transports.File({
                    level: 'silly',
                    filename: path.join(LoggerService.LOG_FOLDER, config.get<string>('logger.name')),
                    maxsize: LoggerService.ONE_MB,
                    maxFiles: 5,
                    tailable: true,
                    format: winston.format.combine(
                        winston.format.timestamp({ format: LoggerService.timestamp }),
                        winston.format.printf(LoggerService.formatter)
                    ),
                }),
                new winston.transports.DailyRotateFile({
                    level: 'info',
                    filename: path.join(LoggerService.LOG_FOLDER, config.get<string>('logger.name')),
                    datePattern: 'YYYY-MM-DD',
                    format: winston.format.combine(
                        winston.format.timestamp({ format: LoggerService.timestamp }),
                        winston.format.printf(LoggerService.formatter)
                    ),
                }),
            ],
        });

        LoggerService.logger.info(`Logger initialized with debug level: ${LoggerService.logLevel}`);
    }

    /**
     * Returns the current timestamp in a formatted string.
     */
    private static timestamp(): string {
        return dateformat(new Date(), 'mmm dd HH:MM:ss');
    }

    /**
     * Formats log messages.
     */
    private static formatter(info: winston.Logform.TransformableInfo): string {
        const levelLabels: Record<string, string> = {
            error: '(E)',
            warn: '(W)',
            info: '(I)',
            verbose: '(V)',
            debug: '(D)',
            silly: '(D)',
        };

        return `${info.timestamp || ''} ${levelLabels[info.level] || '(?)'} ${info.message || ''} ${info.meta && Object.keys(info.meta).length ? JSON.stringify(info.meta) : ''
            }`;
    }

    /**
     * Logs debug messages with a specified debug level.
     */
    public static debug(level: number, ...messages: any[]): boolean {
        if (level <= LoggerService.logLevel) {
            LoggerService.logger.debug(messages.join(' '));
            return true;
        }
        return false;
    }

    /**
     * Sets the log filename and reinitializes transports.
     */
    public static setFilename(filename: string): void {
        LoggerService.logger.clear();

        LoggerService.logger.add(new winston.transports.Console({
            level: 'silly',
            format: winston.format.combine(
                winston.format.timestamp({ format: LoggerService.timestamp }),
                winston.format.printf(LoggerService.formatter)
            ),
        }));

        LoggerService.logger.add(new winston.transports.File({
            level: 'silly',
            filename: path.join(LoggerService.LOG_FOLDER, filename),
            maxsize: LoggerService.ONE_MB,
            maxFiles: 5,
            tailable: true,
            format: winston.format.combine(
                winston.format.timestamp({ format: LoggerService.timestamp }),
                winston.format.printf(LoggerService.formatter)
            ),
        }));

        LoggerService.logger.add(new winston.transports.DailyRotateFile({
            level: 'info',
            filename: path.join(LoggerService.LOG_FOLDER, filename),
            datePattern: 'YYYY-MM-DD',
            format: winston.format.combine(
                winston.format.timestamp({ format: LoggerService.timestamp }),
                winston.format.printf(LoggerService.formatter)
            ),
        }));

        LoggerService.logger.info(`Logger filename updated to: ${filename}`);
    }

    // Logging methods for different levels
    public static error(...messages: any[]): void {
        LoggerService.logger.error(messages.join(' '));
    }

    public static warn(...messages: any[]): void {
        LoggerService.logger.warn(messages.join(' '));
    }

    public static info(...messages: any[]): void {
        LoggerService.logger.info(messages.join(' '));
    }

    public static verbose(...messages: any[]): void {
        LoggerService.logger.verbose(messages.join(' '));
    }
}

export default LoggerService;
