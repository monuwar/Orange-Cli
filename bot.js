const { Worker, isMainThread, workerData, parentPort } = require('worker_threads');
const axios = require('axios');
const cheerio = require('cheerio');
const qs = require('qs');
const fs = require('fs');
const { Telegraf, Markup } = require('telegraf');
const sqlite3 = require('sqlite3').verbose();
const config = require('./config');

const db = new sqlite3.Database('range_tracker.db');
const userDb = new sqlite3.Database('users.db');
const userStates = {}; 
const lastNotifiedTime = {}; 

const emj = (id, fallback) => `<tg-emoji emoji-id="${id}">${fallback}</tg-emoji>`;

process.on('uncaughtException', (err) => {
    console.error(`[FATAL] Uncaught Exception:`, err.message);
});
process.on('unhandledRejection', (reason) => {
    console.error(`[FATAL] Unhandled Rejection:`, reason);
});

if (isMainThread) {
    const bot = new Telegraf(config.BOT_TOKEN);
    let workerFinishedCount = 0;
    let cycleStartTime = Date.now();

    db.serialize(() => {
        db.run("CREATE TABLE IF NOT EXISTS traffic (termination TEXT, prefix TEXT, cli TEXT, country TEXT, timestamp DATETIME, fingerprint TEXT UNIQUE)");
    });

    userDb.serialize(() => {
        userDb.run("CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, name TEXT, username TEXT)");
        userDb.run("CREATE TABLE IF NOT EXISTS watchlist (id INTEGER PRIMARY KEY AUTOINCREMENT, chatId TEXT, monitor_query TEXT, target_country TEXT)");
    });

    const saveUser = (from) => {
        const name = from.first_name + (from.last_name ? ' ' + from.last_name : '');
        const username = from.username ? '@' + from.username : 'No Username';
        userDb.run("INSERT OR REPLACE INTO users (id, name, username) VALUES (?, ?, ?)", [from.id, name, username]);
    };

    setInterval(() => {
        db.run("DELETE FROM traffic WHERE timestamp < datetime('now', '-24 hour')");
    }, 86400000);

    const countryChunks = (array, parts) => {
        let result = [];
        let tempArray = [...array];
        for (let i = parts; i > 0; i--) { result.push(tempArray.splice(0, Math.ceil(tempArray.length / i))); }
        return result;
    };

    const chunks = countryChunks(config.COUNTRIES, 3);

    function attachWorkerHandlers(worker, index) {
        worker.on('message', (msg) => {
            if (msg.type === 'data') {
                db.run("INSERT OR IGNORE INTO traffic VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?)", 
                [msg.term, msg.cli, msg.cli, msg.country, msg.fingerprint], function(err) {
                    if (!err && this.changes > 0) {
                        userDb.all("SELECT * FROM watchlist", [], (err, rows) => {
                            if (!err && rows) {
                                rows.forEach(sub => {
                                    const isQueryMatch = msg.cli.startsWith(sub.monitor_query) || msg.country.toLowerCase() === sub.monitor_query.toLowerCase();
                                    const isTargetMatch = msg.term.toLowerCase().startsWith(sub.target_country.toLowerCase());
                                    if (isQueryMatch && isTargetMatch) {
                                        const alertKey = `${sub.chatId}_${msg.term}_${sub.monitor_query}`;
                                        const now = Date.now();
                                        const lastInfo = lastNotifiedTime[alertKey];
                                        if (!lastInfo || (now - lastInfo.sentAt > 180000 && msg.absTime > lastInfo.absTime)) {
                                            lastNotifiedTime[alertKey] = { absTime: msg.absTime, sentAt: now };
                                            let alertMsg = `${emj('5395695537687123235', '⚠️')} <b>RANGE ALERT FOUND!</b>\n\n`;
                                            alertMsg += `<code>━━━━━━━━━━━━━━━━━━━━━━━━━━</code>\n`;
                                            alertMsg += `${emj('4990182601252668309', '🛰')} <b>Range:</b> <code>${msg.term}</code>\n`;
                                            alertMsg += `${emj('6224104294254124352', '📱')} <b>Test Number:</b> <code>${msg.testNum}</code>\n`;
                                            alertMsg += `${emj('6224421898495729165', '🔥')} <b>Status:</b> <code>Found ${msg.timeText}</code>\n`;
                                            alertMsg += `${emj('5447410659077661506', '🌍')} <b>Source:</b> <code>${msg.cli}</code> (${msg.country})\n`;
                                            alertMsg += `<code>━━━━━━━━━━━━━━━━━━━━━━━━━━</code>\n`;
                                            alertMsg += `${emj('5258474669769497337', '⏹')} <b>Stop alert:</b> /stop_${sub.target_country.replace(/\s+/g, '_').toLowerCase()}_${sub.monitor_query.replace(/\s+/g, '_').toLowerCase()}`;
                                            // ─── FIX #3: sendMessage এ .catch() যোগ ─────────────────────
                                            bot.telegram.sendMessage(sub.chatId, alertMsg, { parse_mode: 'HTML' }).catch((e) => {
                                                console.error(`[Bot] Alert send failed to ${sub.chatId}:`, e.message);
                                            });
                                            // ────────────────────────────────────────────────────────────
                                        }
                                    }
                                });
                            }
                        });
                    }
                });
            } else if (msg.type === 'cycle_done') {
                workerFinishedCount++;
                // ─── FIX #4: hardcoded 3 এর বদলে chunks.length ──────────────
                if (workerFinishedCount === chunks.length) {
                    let totalDur = ((Date.now() - cycleStartTime) / 1000).toFixed(0);
                    console.log(`[System] FULL Cycle Completed in ${Math.floor(totalDur/60)}m ${totalDur%60}s.`);
                    workerFinishedCount = 0; cycleStartTime = Date.now();
                }
                // ────────────────────────────────────────────────────────────
            }
        });

        worker.on('error', (err) => {
            console.error(`[Worker ${index + 1}] Error: ${err.message}. Restarting in 10s...`);
        });

        worker.on('exit', (code) => {
            if (code !== 0) {
                console.error(`[Worker ${index + 1}] Exited with code ${code}. Restarting in 10s...`);
                setTimeout(() => {
                    console.log(`[Worker ${index + 1}] Restarting...`);
                    const newWorker = new Worker(__filename, { workerData: { countries: chunks[index], workerId: index + 1 } });
                    attachWorkerHandlers(newWorker, index);
                }, 10000);
            }
        });
    }

    async function startWorkers() {
        for (let [index, chunk] of chunks.entries()) {
            const worker = new Worker(__filename, { workerData: { countries: chunk, workerId: index + 1 } });
            attachWorkerHandlers(worker, index);
            console.log(`[System] API Worker ${index + 1} started.`);
            await new Promise(r => setTimeout(r, 120000)); 
        }
    }
    startWorkers();

    const getRanking = (limit, minutes) => {
        return new Promise((resolve) => {
            const timeLabel = minutes === 30 ? "30min" : "1H";
            const query = `SELECT termination, COUNT(*) as hits FROM traffic WHERE timestamp >= datetime('now', '-${minutes} minute') GROUP BY termination ORDER BY hits DESC LIMIT ${limit}`;
            db.all(query, [], (err, rows) => {
                if (err || !rows.length) return resolve(`${emj('6224039100945538099', '❌')} <b>No data found.</b>`);
                let text = `${emj('5264919878082509254', '📊')} <b>Top ${limit} Range (${timeLabel})</b>\n\n<code>━━━━━━━━━━━━━━━━━━━━━━</code>\n`;
                rows.forEach((row, i) => {
                    let icon = emj('4990298741463319592', '💠');
                    if (i === 0) icon = emj('5440539497383087970', '1️⃣');
                    else if (i === 1) icon = emj('5447203607294265305', '2️⃣');
                    else if (i === 2) icon = emj('5453902265922376865', '3️⃣');
                    text += `${icon} <code>${row.termination}</code> ➜ <b>${row.hits} Hits</b>\n`;
                });
                resolve(text);
            });
        });
    };

    const getCountryReport = (searchTerm, userId) => {
        return new Promise((resolve) => {
            const query = "SELECT * FROM traffic WHERE (LOWER(termination) LIKE (LOWER(?) || '%') OR prefix LIKE (LOWER(?) || '%')) AND timestamp >= datetime('now', '-60 minute')";
            db.all(query, [searchTerm, searchTerm], (err, rows) => {
                if (err || !rows.length) return resolve(`${emj('6224039100945538099', '❌')} <b>No data found for: ${searchTerm}</b>`);
                const rangesMap = {}; rows.forEach(r => { rangesMap[r.termination] = (rangesMap[r.termination] || 0) + 1; });
                const topRanges = Object.entries(rangesMap).sort((a, b) => b[1] - a[1]).slice(0, 10);

                let text = `${emj('5447410659077661506', '🌍')} <b>DETAILED REPORT: ${searchTerm.toUpperCase()}</b>\n\n`;
                text += `<code>━━━━━━━━━━━━━━━━━━━━━━━━━━</code>\n`;
                text += `${emj('5258200019495821936', '💠')} <b>TOTAL HITS (1H):</b> <code>${rows.length}</code>\n`;
                text += `<code>━━━━━━━━━━━━━━━━━━━━━━━━━━</code>\n`;
                text += `${emj('5341715473882955310', '⚙️')} <b>TOP 10 RANGES:</b>\n\n`;
                topRanges.forEach(([term, hit]) => { 
                    text += `┣ ${emj('6287183104540943012', '📶')} <code>${term}</code> ➜ <b>[${hit}]</b>\n`; 
                });

                if (String(userId) === String(config.ADMIN_ID)) {
                    const clisMap = {}; rows.forEach(r => { clisMap[r.cli] = (clisMap[r.cli] || 0) + 1; });
                    const topClis = Object.entries(clisMap).sort((a, b) => b[1] - a[1]).slice(0, 10);
                    text += `\n<code>━━━━━━━━━━━━━━━━━━━━━━━━━━</code>\n`;
                    text += `${emj('5341715473882955310', '⚙️')} <b>TOP ACTIVE CLIs:</b>\n\n`;
                    topClis.forEach(([cli, hit]) => { 
                        text += `┣ ${emj('4990298741463319592', '💠')} <code>${cli}</code> ➜ <b>[${hit}] Hits</b>\n`; 
                    });
                }
                text += `<code>━━━━━━━━━━━━━━━━━━━━━━━━━━</code>`;
                resolve(text);
            });
        });
    };

    const mainKeyboard = {
        reply_markup: {
            keyboard: [
                [
                    { text: "Top 20 Range (30M)", style: "primary", icon_custom_emoji_id: "6221943272869207790" },
                    { text: "Top 20 Range (1H)", style: "success", icon_custom_emoji_id: "6221840983928085928" }
                ],
                [
                    { text: "Top 50 Range", style: "danger", icon_custom_emoji_id: "6222203784110546425" },
                    { text: "Country Prefix", style: "danger", icon_custom_emoji_id: "5231012545799666522" }
                ],
                [
                    { text: "Target Range Notification", style: "success", icon_custom_emoji_id: "5458603043203327669" }
                ]
            ],
            resize_keyboard: true, is_persistent: true
        }
    };

    bot.use((ctx, next) => { if (ctx.chat?.type === 'private') return next(); });

    bot.command('user', (ctx) => {
        if (String(ctx.from.id) !== String(config.ADMIN_ID)) return;
        userDb.all("SELECT * FROM users", [], (err, rows) => {
            let msg = `👥 <b>BOT USER LOG</b>\n━━━━━━━━━━━━━━━━━━\n`;
            if (rows) rows.forEach((row, i) => { msg += `${i + 1}. <b>${row.name}</b> (${row.username})\n`; });
            ctx.replyWithHTML(msg);
        });
    });

    bot.command('bc', async (ctx) => {
        if (String(ctx.from.id) !== String(config.ADMIN_ID)) return;
        const msg = ctx.message.text.split(' ').slice(1).join(' ');
        userDb.all("SELECT id FROM users", [], (err, rows) => {
            if (rows) rows.forEach(u => bot.telegram.sendMessage(u.id, msg).catch(() => {}));
            ctx.reply("Broadcast sent.");
        });
    });

    bot.start((ctx) => {
        saveUser(ctx.from);
        ctx.replyWithHTML(`Hello! Welcome back. I'm currently tracking live traffic for you.`, {
            reply_markup: {
                keyboard: mainKeyboard.reply_markup.keyboard,
                resize_keyboard: true,
                is_persistent: true,
                inline_keyboard: [[
                    { text: "Contact Developer", url: 'https://t.me/imonuwar', style: 'danger', icon_custom_emoji_id: '5229027828527309057' }
                ]]
            }
        });
    });

    bot.hears('Top 20 Range (30M)', async (ctx) => { delete userStates[ctx.from.id]; ctx.replyWithHTML(await getRanking(20, 30)); });
    bot.hears('Top 20 Range (1H)', async (ctx) => { delete userStates[ctx.from.id]; ctx.replyWithHTML(await getRanking(20, 60)); });
    bot.hears('Top 50 Range', async (ctx) => { delete userStates[ctx.from.id]; ctx.replyWithHTML(await getRanking(50, 30)); });
    bot.hears('Country Prefix', (ctx) => { userStates[ctx.from.id] = 'await_search'; ctx.replyWithHTML(`Just type the <b>Country Name</b> or <b>Prefix</b> you'd like to check`); });
    
    bot.hears('Target Range Notification', (ctx) => {
        userStates[ctx.from.id] = 'setup_1';
        ctx.replyWithHTML(`${emj('6221855350593691363', '🔔')} <b>NOTIFICATION SETUP</b>\n\nPlease enter the <b>Prefix</b> or <b>Country Name</b> you want to monitor:`);
    });

    bot.hears(/^\/stop_(.+)/, (ctx) => {
        const cmd = ctx.match[1].split('_'); const query = cmd.pop(); const country = cmd.join(' ').replace(/_/g, ' ');
        userDb.run("DELETE FROM watchlist WHERE chatId = ? AND monitor_query = ? AND target_country LIKE ?", [ctx.from.id, query, country + '%'], () => { 
            ctx.replyWithHTML(`${emj('5258474669769497337', '⏹')} <b>Notification stopped</b> for ${country.toUpperCase()} from ${query}.`); 
        });
    });

    bot.on('text', async (ctx) => {
        const txt = ctx.message.text, state = userStates[ctx.from.id];
        if (['Top 20 Range (30M)', 'Top 20 Range (1H)', 'Top 50 Range', 'Country Prefix', 'Target Range Notification'].includes(txt) || txt.startsWith('/')) return;
        if (state === 'await_search') {
            saveUser(ctx.from); ctx.replyWithHTML(await getCountryReport(txt, ctx.from.id)); delete userStates[ctx.from.id];
        } else if (state === 'setup_1') {
            userStates[ctx.from.id] = { step: 2, query: txt };
            ctx.replyWithHTML(`${emj('5258152182150077732', '🎯')} <b>TARGET SET</b>\n\nNow, type the <b>Country Name</b> whose range you are waiting for:`);
        } else if (state && state.step === 2) {
            userDb.run("INSERT INTO watchlist (chatId, monitor_query, target_country) VALUES (?, ?, ?)", [ctx.from.id, state.query, txt]);
            ctx.replyWithHTML(`${emj('5251203410396458957', '✅')} <b>WATCHLIST ACTIVE</b>\n\nMonitoring started! I will notify you when a match is found.`);
            delete userStates[ctx.from.id];
        }
    });

    bot.catch((err, ctx) => {
        console.error(`[Bot] Polling/Handler Error:`, err.message);
    });

    bot.launch();

} else {
    const { countries, workerId } = workerData; let isFirstCycle = true;

    function parseToMs(text) { let m = text.match(/(\d+)\s+(second|minute|hour)/); if (!m) return 0; let val = parseInt(m[1]); if (m[2] === 'second') return val * 1000; if (m[2] === 'minute') return val * 60000; return val * 3600000; }

    async function axiosWithRetry(url, data, headers, retries = 4) {
        for (let attempt = 0; attempt <= retries; attempt++) {
            try {
                const response = await axios.post(url, data, {
                    headers,
                });
                return response;
            } catch (err) {
                const isTimeout = err.code === 'ETIMEDOUT' || err.code === 'ECONNABORTED' || err.code === 'ECONNRESET' || err.code === 'ENOTFOUND';
                if (attempt < retries) {
                    const waitMs = delays[attempt];
                    console.log(`[Worker ${workerId}] ${isTimeout ? 'TIMEOUT' : 'ERROR'} — Retry ${attempt + 1}/${retries} in ${waitMs/1000}s...`);
                    await new Promise(r => setTimeout(r, waitMs));
                } else {
                }
            }
        }
    }

    async function runWorker() {
        while (true) {
            let cycleStart = Date.now(), session;
            try { session = JSON.parse(fs.readFileSync('./session.json', 'utf8')); } catch (e) { await new Promise(r => setTimeout(r, 5000)); continue; }
            const apiHeaders = { 'content-type': 'application/x-www-form-urlencoded; charset=UTF-8', 'cookie': session.COOKIE, 'x-csrf-token': session.CSRF_TOKEN, 'x-requested-with': 'XMLHttpRequest', 'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36' };
            for (const country of countries) {
                try {
                    // ─── FIX #7 ব্যবহার: axiosWithRetry দিয়ে call ───────────
                    const response = await axiosWithRetry(
                        'https://www.orangecarrier.com/services/cli/access/get',
                        qs.stringify({ q: country }),
                        apiHeaders
                    );
                    // ─────────────────────────────────────────────────────────
                    if (response.data.toLowerCase().includes('no result')) { console.log(`[Worker ${workerId}] SKIP: ${country}`); continue; }
                    const $ = cheerio.load(`<table>${response.data}</table>`);
                    let taken = 0;
                    $('tr').each((i, el) => {
                        const cells = $(el).find('td');
                        if (cells.length >= 6) {
                            const term = $(cells[0]).text().trim(), testNum = $(cells[1]).text().trim(), cli = $(cells[3]).text().trim(), time = $(cells[5]).text().trim();
                            let isStale = (time.includes('minute') && parseInt(time) >= 3) || time.includes('hour') || time.includes('day');
                            let shouldTake = isFirstCycle ? !time.includes('hour') && !time.includes('day') : !isStale;
                            if (shouldTake && cli && cli !== "No data available") {
                                let absTime = Math.floor((Date.now() - parseToMs(time)) / 1000);
                                parentPort.postMessage({ type: 'data', term, cli, testNum, country, timeText: time, absTime, fingerprint: `${term}|${testNum}|${cli}|${absTime}` });
                                taken++;
                            }
                        }
                    });
                    console.log(`[Worker ${workerId}] SUCCESS: ${country} (${taken} taken)`);
                    await new Promise(r => setTimeout(r, 400));
                } catch (err) { 
                    console.log(`[Worker ${workerId}] FAILED after retries: ${country} — ${err.message}`);
                }
            }
            isFirstCycle = false; parentPort.postMessage({ type: 'cycle_done' });
            let waitTime = 60000 - (Date.now() - cycleStart);
            if (waitTime > 0) await new Promise(r => setTimeout(r, waitTime));
        }
    }
    runWorker();
}
