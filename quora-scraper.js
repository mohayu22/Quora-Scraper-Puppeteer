const puppeteer = require('puppeteer');
const { parse } = require('json2csv');
const winston = require('winston');
const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');

const LOCATION = "us";
const MAX_RETRIES = 3;
const MAX_SEARCH_DATA_THREADS = 2;
const MAX_ANSWER_DATA_THREADS = 2;
const MAX_SEARCH_RESULTS = 4;

const keywords = ["What is nextTick in NodeJS"];

const { api_key: API_KEY } = JSON.parse(fs.readFileSync('config.json', 'utf8'));

function getScrapeOpsUrl(url, location = LOCATION) {
    const params = new URLSearchParams({
        api_key: API_KEY,
        url,
        country: location,
        wait: 5000,
    });
    return `https://proxy.scrapeops.io/v1/?${params.toString()}`;
}

const logger = winston.createLogger({
    level: 'info',
    format: winston.format.combine(
        winston.format.timestamp(),
        winston.format.printf(({ timestamp, level, message }) => {
            return `${timestamp} [${level.toUpperCase()}]: ${message}`;
        })
    ),
    transports: [
        new winston.transports.Console(),
        new winston.transports.File({ filename: 'quora-scraper.log' })
    ]
});

class SearchData {
    constructor(data = {}) {
        this.rank = data.rank || 0;
        this.title = this.validateTitle(data.title);
        this.url = this.validateUrl(data.url);
    }

    validateTitle(title) {
        if (typeof title === 'string' && title.trim() !== '') {
            return title.trim();
        }
        return "No title";
    }

    validateUrl(url) {
        try {
            return (url && new URL(url).hostname) ? url.trim() : "Invalid URL";
        } catch {
            return "Invalid URL";
        }
    }
}

class AnswersData {
    constructor(data = {}) {
        this.title = this.validateAuthor(data.title);
        this.answer = this.validateAnswer(data.answer);
    }

    validateAuthor(author) {
        if (typeof author === 'string' && author.trim() !== '') {
            return author.trim();
        }
        return "No author";
    }

    validateAnswer(answer) {
        if (typeof answer === 'string' && answer.trim() !== '') {
            return answer.trim();
        }
        return "No answer";
    }
}

class DataPipeline {
    constructor(csvFilename, storageQueueLimit = 50) {
        this.namesSeen = new Set();
        this.storageQueue = [];
        this.storageQueueLimit = storageQueueLimit;
        this.csvFilename = csvFilename;
    }

    async saveToCsv() {
        const filePath = path.resolve(this.csvFilename);
        const dataToSave = this.storageQueue.splice(0, this.storageQueue.length);
        if (dataToSave.length === 0) return;

        const csvData = parse(dataToSave, { header: !fs.existsSync(filePath) });
        fs.appendFileSync(filePath, csvData + '\n', 'utf8');
    }

    isDuplicate(title) {
        if (this.namesSeen.has(title)) {
            logger.warn(`Duplicate item found: ${title}. Item dropped.`);
            return true;
        }
        this.namesSeen.add(title);
        return false;
    }

    async addData(data) {
        if (!this.isDuplicate(data.title)) {
            this.storageQueue.push(data);
            if (this.storageQueue.length >= this.storageQueueLimit) {
                await this.saveToCsv();
            }
        }
    }

    async closePipeline() {
        if (this.storageQueue.length > 0) await this.saveToCsv();
    }
}

async function scrapeSearchResults(dataPipeline, url, retries = MAX_RETRIES) {
    const browser = await puppeteer.launch({ headless: true });
    const page = await browser.newPage();
    let searchResults;

    while (retries > 0) {
        try {
            await page.goto(getScrapeOpsUrl(url), { waitUntil: 'domcontentloaded' });

            searchResults = await page.$$eval('#rso .g', results => {
                return results.map((result, index) => {
                    const title = result.querySelector('h3') ? result.querySelector('h3').innerText : null;
                    const url = result.querySelector('a') ? result.querySelector('a').href : null;
                    const rank = index + 1;
                    return { rank, title, url };
                });
            });

            if (searchResults.length > 0) {
                break;
            }
        } catch (error) {
            console.log(`Error scraping ${url}. Retries left: ${retries}`);
            retries--;

            if (retries > 0) {
                await new Promise(resolve => setTimeout(resolve, 2000));
            }
        }
    }

    if (!searchResults || searchResults.length === 0) {
        logger.error(`Failed to scrape results for ${url} after multiple attempts.`);
        await browser.close();
        return;
    }

    searchResults = searchResults.filter(({ url }) => url.startsWith('https://www.quora.com/') && !url.includes('/profile'));
    for (const result of searchResults) {
        await dataPipeline.addData(new SearchData(result));
    }

    logger.info(`Successfully Scraped ${searchResults.length} Search Results for ${dataPipeline.csvFilename.match(/([^/]+\.csv)/)[0]}`);
    await browser.close();
}


async function scrapeAnswersData(dataPipeline, url, retries = MAX_RETRIES) {
    const browser = await puppeteer.launch({ headless: true });
    let totalAnswers = 0;

    const page = await browser.newPage();
    while (retries > 0) {
        try {
            console.log(`Attempting to scrape ${url}, Retries left: ${retries}`);
            await page.goto(getScrapeOpsUrl(url), { waitUntil: "networkidle2" });

            await page.waitForSelector('button[aria-haspopup="menu"][role="button"]', { state: 'attached' });
            await page.click('button[aria-haspopup="menu"][role="button"]');
            await page.waitForSelector('.puppeteer_test_popover_menu');
            const options = await page.$$('.q-click-wrapper.puppeteer_test_popover_item');
            await options[1].click();

            await autoScroll(page);

            const answers = await page.evaluate(() => {
                const answerElements = document.querySelectorAll('[class^="q-box dom_annotate_question_answer_item_"]');
                return Array.from(answerElements).map((answer) => {
                    const readMoreButton = answer.querySelector('button.puppeteer_test_read_more_button');

                    if (readMoreButton) {
                        readMoreButton.click();
                    }

                    const authorName = answer.querySelector('.q-box.spacing_log_answer_header');
                    const authorNameText = authorName ? authorName.innerText.split("\n")[0] : "Unknown Author";

                    const authorAnswer = answer.querySelector('.q-box.spacing_log_answer_content.puppeteer_test_answer_content').innerText;

                    return { title: authorNameText, answer: authorAnswer };
                });
            });

            totalAnswers = answers.length;

            answers.forEach((answer) => {
                dataPipeline.addData(new AnswersData(answer));
            });

            break;
        } catch (error) {
            console.error(`Error scraping ${url}. Retries left: ${retries}`);
            retries--;

            if (retries > 0) {
                console.log('Retrying...');
                await new Promise(resolve => setTimeout(resolve, 2000));
            } else {
                console.error(`Failed to scrape ${url} after multiple retries.`);
            }
        }
    }

    logger.info(`Successfully Scraped ${totalAnswers} Answers for ${url}`);
    await browser.close();
}


async function autoScroll(page) {
    await page.evaluate(async () => {
        await new Promise((resolve) => {
            let totalHeight = 0;
            const distance = 100;
            const timer = setInterval(() => {
                window.scrollBy(0, distance);
                totalHeight += distance;
                if (totalHeight >= document.body.scrollHeight) {
                    clearInterval(timer);
                    resolve();
                }
            }, 100);
        });
    });
}

async function readCsvAndGetUrls(csvFilename) {
    return new Promise((resolve, reject) => {
        const urls = [];
        fs.createReadStream(csvFilename)
            .pipe(csv())
            .on('data', (row) => {
                if (row.url) urls.push(row.url);
            })
            .on('end', () => resolve(urls))
            .on('error', reject);
    });
}

const scrapeConcurrently = async (tasks, maxConcurrency) => {
    const results = [];
    const executing = new Set();

    for (const task of tasks) {
        const promise = task().then(result => {
            executing.delete(promise);
            return result;
        });
        executing.add(promise);
        results.push(promise);

        if (executing.size >= maxConcurrency) {
            await Promise.race(executing);
        }
    }

    return Promise.all(results);
};

async function getAllUrlsFromFiles(files) {
    const urls = [];
    for (const file of files) {
        urls.push(...await readCsvAndGetUrls(file));
    }
    return urls;
}

async function main() {
    logger.info("Started Scraping Search Results")

    const aggregateFiles = [];

    const scrapeSearchResultsTasks = keywords.map(keyword => async () => {
        const url = `https://www.google.com/search?q=${encodeURIComponent(keyword)}+site:quora.com&num=${MAX_SEARCH_RESULTS}`;
        const fileName = path.resolve(keyword.replace(/\s+/g, '-').toLowerCase() + "-search-results.csv");

        const dataPipeline = new DataPipeline(fileName);

        await scrapeSearchResults(dataPipeline, url);
        await dataPipeline.closePipeline();
        aggregateFiles.push(fileName);
    });

    await scrapeConcurrently(scrapeSearchResultsTasks, MAX_SEARCH_DATA_THREADS);

    const urls = await getAllUrlsFromFiles(aggregateFiles);

    const scrapeAnswersTasks = urls.map(url => async () => {
        const dataPipeline = new DataPipeline(url.match(/([^/]+)$/)[1] + "-Answers.csv");
        await scrapeAnswersData(dataPipeline, url);
        await dataPipeline.closePipeline();
    });

    await scrapeConcurrently(scrapeAnswersTasks, MAX_ANSWER_DATA_THREADS);
}

main();
