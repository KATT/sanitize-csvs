const fs = require('fs').promises;
const path = require('path');
const readline = require('readline');
const { createReadStream, unlink } = require('fs');
const sqlite3 = require('sqlite3').verbose();

async function openDatabase() {
    const dbPath = 'output/database.sqlite';
    
    // Delete existing database if it exists
    try {
        await fs.unlink(dbPath);
    } catch (error) {
        if (error.code !== 'ENOENT') throw error;
    }

    // Create and open new database
    return new Promise((resolve, reject) => {
        const db = new sqlite3.Database(dbPath, (err) => {
            if (err) reject(err);
            else resolve(db);
        });
    });
}

async function createTable(db, tableName, headers) {
    const columns = headers.map(header => 
        `"${header}" TEXT`
    ).join(', ');
    
    return new Promise((resolve, reject) => {
        const sql = `CREATE TABLE IF NOT EXISTS "${tableName}" (${columns})`;
        db.run(sql, (err) => {
            if (err) reject(err);
            else resolve();
        });
    });
}

async function insertBatch(db, tableName, rows) {
    if (rows.length === 0) return;
    
    const placeholders = rows.map(() => 
        `(${new Array(rows[0].length).fill('?').join(',')})`
    ).join(',');
    
    const sql = `INSERT INTO "${tableName}" VALUES ${placeholders}`;
    const values = rows.flat();
    
    return new Promise((resolve, reject) => {
        db.run(sql, values, (err) => {
            if (err) reject(err);
            else resolve();
        });
    });
}

function formatProgress(current, total, width = 30) {
    const percentage = Math.floor((current / total) * 100);
    const filled = Math.floor((width * current) / total);
    const empty = width - filled;
    const bar = '█'.repeat(filled) + '░'.repeat(empty);
    return `${bar} ${percentage}% (${current}/${total})`;
}

async function processCSVFile(db, filePath, tableName, progressCallback) {
    console.log(`Started processing ${tableName}`);
    const fileStream = createReadStream(filePath);
    const rl = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity
    });

    // Get total lines first
    let totalLines = 0;
    for await (const _ of rl) {
        totalLines++;
    }
    
    // Reset stream
    fileStream.destroy();
    const newFileStream = createReadStream(filePath);
    rl.close();
    const newRl = readline.createInterface({
        input: newFileStream,
        crlfDelay: Infinity
    });

    let isFirstLine = true;
    let lineNumber = 0;
    let headers;
    let errorCount = 0;
    let skippedCount = 0;
    let batch = [];
    const BATCH_SIZE = 100;
    let lastProgressUpdate = Date.now();
    const errors = []; // Collect all errors

    try {
        for await (const line of newRl) {
            lineNumber++;
            
            // Update progress every 100ms to avoid console spam
            const now = Date.now();
            if (now - lastProgressUpdate > 100) {
                progressCallback(lineNumber, totalLines);
                lastProgressUpdate = now;
            }

            const fields = line.split('|').map(field => 
                field.replace(/^"/, '').replace(/"$/, '').trim()
            );

            if (isFirstLine) {
                headers = fields;
                await createTable(db, tableName, headers);
                isFirstLine = false;
                continue;
            }

            // Skip lines with incorrect number of columns
            if (fields.length !== headers.length) {
                skippedCount++;
                errors.push({
                    type: 'column_mismatch',
                    line: lineNumber,
                    expected: headers.length,
                    got: fields.length,
                    content: line
                });
                continue;
            }

            batch.push(fields);

            if (batch.length >= BATCH_SIZE) {
                try {
                    await insertBatch(db, tableName, batch);
                } catch (error) {
                    errorCount++;
                    errors.push({
                        type: 'insert_error',
                        line: lineNumber - batch.length + 1,
                        message: error.message,
                        batchSize: batch.length
                    });
                }
                batch = [];
            }
        }

        // Insert any remaining rows
        if (batch.length > 0) {
            try {
                await insertBatch(db, tableName, batch);
            } catch (error) {
                errorCount++;
                errors.push({
                    type: 'insert_error',
                    line: lineNumber - batch.length + 1,
                    message: error.message,
                    batchSize: batch.length
                });
            }
        }

        return {
            file: tableName,
            totalLines: lineNumber,
            errorCount,
            skippedCount,
            errors
        };
    } catch (error) {
        console.error(`\nFatal error processing ${tableName}:`, error);
        throw error;
    }
}

async function main() {
    try {
        const db = await openDatabase();
        const outputDir = 'output';
        const files = await fs.readdir(outputDir);
        const csvFiles = files.filter(file => file.toLowerCase().endsWith('.csv'));

        console.log(`Found ${csvFiles.length} CSV files to process\n`);

        let totalLinesProcessed = 0;
        let totalLinesAcrossFiles = 0;
        const progressByFile = new Map();

        // Process files in parallel
        const processPromises = csvFiles.map(file => {
            const filePath = path.join(outputDir, file);
            const tableName = path.basename(file, '.csv').replace(/[^a-zA-Z0-9]/g, '_');
            
            return processCSVFile(
                db, 
                filePath, 
                tableName,
                (lines, total) => {
                    progressByFile.set(file, { lines, total });
                    
                    // Calculate overall progress
                    totalLinesProcessed = Array.from(progressByFile.values())
                        .reduce((sum, { lines }) => sum + lines, 0);
                    totalLinesAcrossFiles = Array.from(progressByFile.values())
                        .reduce((sum, { total }) => sum + total, 0);

                    // Clear and redraw progress
                    process.stdout.write('\x1B[2J\x1B[0f');
                    console.log('Overall progress:');
                    console.log(formatProgress(totalLinesProcessed, totalLinesAcrossFiles));
                    
                    console.log('\nProgress by file:');
                    for (const [fname, progress] of progressByFile) {
                        console.log(`${fname}:`);
                        console.log(formatProgress(progress.lines, progress.total));
                    }
                }
            );
        });

        // Wait for all files to be processed
        const results = await Promise.all(processPromises);
        
        // Print error summary
        console.log('\n=== Error Summary ===');
        
        for (const result of results) {
            console.log(`\nFile: ${result.file}`);
            console.log(`Total lines: ${result.totalLines}`);
            console.log(`Errors: ${result.errorCount}`);
            console.log(`Skipped: ${result.skippedCount}`);
            
            if (result.errors.length > 0) {
                console.log('\nDetailed Errors:');
                
                // Group errors by type
                const columnErrors = result.errors.filter(e => e.type === 'column_mismatch');
                const insertErrors = result.errors.filter(e => e.type === 'insert_error');
                
                if (columnErrors.length > 0) {
                    console.log('\nColumn count mismatches:');
                    columnErrors.slice(0, 5).forEach(err => {
                        console.log(`Line ${err.line}: Expected ${err.expected} columns, got ${err.got}`);
                        console.log(`Content: ${err.content}`);
                    });
                    if (columnErrors.length > 5) {
                        console.log(`... and ${columnErrors.length - 5} more column errors`);
                    }
                }
                
                if (insertErrors.length > 0) {
                    console.log('\nInsertion errors:');
                    insertErrors.slice(0, 5).forEach(err => {
                        console.log(`Lines ${err.line}-${err.line + err.batchSize - 1}: ${err.message}`);
                    });
                    if (insertErrors.length > 5) {
                        console.log(`... and ${insertErrors.length - 5} more insertion errors`);
                    }
                }
            }
        }

        // Close database connection
        await new Promise((resolve, reject) => {
            db.close((err) => {
                if (err) reject(err);
                else resolve();
            });
        });

        console.log('\nProcessing complete!');
    } catch (error) {
        console.error('Fatal error:', error);
        process.exit(1);
    }
}

main();
