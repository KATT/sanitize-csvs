const fs = require('fs').promises;
const path = require('path');
const readline = require('readline');
const { createReadStream, createWriteStream } = require('fs');

async function processFile(inputPath, outputPath) {
    // Remove output file if it exists
    try {
        await fs.unlink(outputPath);
    } catch (error) {
        // Ignore error if file doesn't exist
        if (error.code !== 'ENOENT') throw error;
    }

    const fileStream = createReadStream(inputPath);
    const rl = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity
    });

    const writeStream = createWriteStream(outputPath);
    const filename = path.basename(inputPath);
    let isFirstLine = true;
    let lastLine = '';
    let lineNumber = 0;

    // Process the file line by line
    for await (const line of rl) {
        lineNumber++;
        if (!isFirstLine) {
            // Write the previous line with a newline
            writeStream.write(lastLine + '\n');
        }
        isFirstLine = false;
        
        // Split the line into fields
        const fields = line.split('*|*');
        
        // Get expected number of columns from first line
        if (lineNumber === 1) {
            expectedColumns = fields.length;
        }
        
        // Skip lines with wrong number of columns
        if (fields.length !== expectedColumns) {
            console.warn(`Error on line #${lineNumber}: Wrong number of columns (expected ${expectedColumns}, got ${fields.length})`);
            console.warn(`-> Line content: ${line}`);
            continue;
        }

        // Quote each field and replace the separator
        lastLine = fields.map(field => {
            return `"${field.replace(/"/g, '').trim()}"`;
        }).join('|');
    }

    // Write the last line if it was valid
    if (lastLine) {
        writeStream.write(lastLine + '\n');
    }

    writeStream.end();
}

async function main() {
    try {
        // Create output directory if it doesn't exist
        await fs.mkdir('output', { recursive: true });

        // Read all files from the input directory
        const files = await fs.readdir('input');
        
        // Filter for CSV files
        const csvFiles = files.filter(file => file.toLowerCase().endsWith('.csv'));

        console.log(`Found ${csvFiles.length} CSV files to process`);

        // Process each file
        for (const file of csvFiles) {
            const inputPath = path.join('input', file);
            const outputPath = path.join('output', file);

            console.log(`Processing ${file}...`);
            await processFile(inputPath, outputPath);
            console.log(`Finished processing ${file}`);
        }

        console.log('All files processed successfully!');
    } catch (error) {
        console.error('Error processing files:', error);
    }
}

main();
