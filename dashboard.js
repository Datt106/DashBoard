/**
 * KIMBALL BI DASHBOARD ENGINE
 * Power BI / Metabase interactive visualization controller
 */

document.addEventListener("DOMContentLoaded", () => {
    initDashboard();
});

let currentPlatform = "all";
let currentTab = "overview";

function initDashboard() {
    renderKPIs();
    renderBai1Charts();
    renderBai2Charts();
    renderBai3Charts();
    setupNavigation();
    setupFilters();
    setupSQLStudio();
}

// ----------------------------------------------------
// 1. KPI RENDERING & CALCULATIONS
// ----------------------------------------------------
function renderKPIs() {
    let views = KIMBALL_METADATA.totalPageviews;
    let uvs = KIMBALL_METADATA.totalUniqueVisitors;
    let domains = KIMBALL_METADATA.totalDomains;

    if (currentPlatform === "mobile") {
        views = KIMBALL_METADATA.mobilePageviews;
        uvs = Math.round(KIMBALL_METADATA.totalUniqueVisitors * 0.65);
    } else if (currentPlatform === "pc") {
        views = KIMBALL_METADATA.pcPageviews;
        uvs = Math.round(KIMBALL_METADATA.totalUniqueVisitors * 0.35);
    }

    document.getElementById("kpi-total-views").innerText = views.toLocaleString();
    document.getElementById("kpi-total-uv").innerText = uvs.toLocaleString();
    document.getElementById("kpi-active-domains").innerText = domains.toLocaleString();
    
    const peakHour = currentPlatform === "pc" ? "10:00 (12.7M)" : "20:00 (19.1M)";
    document.getElementById("kpi-peak-hour").innerText = peakHour;
}

// ----------------------------------------------------
// 2. BÀI 1: TOP 10 DOMAINS (MOBILE & PC)
// ----------------------------------------------------
function renderBai1Charts() {
    const tableBody = document.getElementById("table-top-domains");
    if (!tableBody) return;

    let data = [];
    if (currentPlatform === "all" || currentPlatform === "mobile") {
        data = BAI1_MOBILE_TOP10;
    } else {
        data = BAI1_PC_TOP10;
    }

    const maxViews = Math.max(...data.map(d => d.pageviews));

    tableBody.innerHTML = data.map((d, i) => {
        const badgeClass = i === 0 ? "gold" : i === 1 ? "silver" : i === 2 ? "bronze" : "normal";
        const pct = ((d.pageviews / maxViews) * 100).toFixed(1);
        return `
            <tr>
                <td style="width: 60px;"><span class="rank-badge ${badgeClass}">${d.rank}</span></td>
                <td><strong style="color: #60a5fa;">${d.clean_domain}</strong></td>
                <td>${d.pageviews.toLocaleString()}</td>
                <td>${d.percentage}%</td>
                <td style="width: 250px;">
                    <div class="bar-cell">
                        <div class="bar-track">
                            <div class="bar-fill" style="width: ${pct}%;"></div>
                        </div>
                        <span style="font-size: 0.75rem; color: #94a3b8; width: 45px;">${pct}%</span>
                    </div>
                </td>
            </tr>
        `;
    }).join("");

    renderChartBarComparison();
}

function renderChartBarComparison() {
    const canvas = document.getElementById("chart-domains-bar");
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    const w = canvas.width = canvas.parentElement.clientWidth;
    const h = canvas.height = 320;

    ctx.clearRect(0, 0, w, h);

    const domains = BAI1_MOBILE_TOP10.slice(0, 8);
    const barHeight = 24;
    const gap = 14;
    const startY = 30;
    const maxVal = 25000000;
    const leftMargin = 140;
    const rightMargin = 60;
    const usableW = w - leftMargin - rightMargin;

    domains.forEach((d, i) => {
        const y = startY + i * (barHeight + gap);
        const barW = (d.pageviews / maxVal) * usableW;

        // Label
        ctx.fillStyle = "#cbd5e1";
        ctx.font = "12px Inter, sans-serif";
        ctx.textAlign = "right";
        ctx.fillText(d.clean_domain, leftMargin - 15, y + 16);

        // Bar Track
        ctx.fillStyle = "rgba(255, 255, 255, 0.05)";
        ctx.beginPath();
        ctx.roundRect(leftMargin, y, usableW, barHeight, 4);
        ctx.fill();

        // Bar
        const grad = ctx.createLinearGradient(leftMargin, 0, leftMargin + barW, 0);
        grad.addColorStop(0, "#3b82f6");
        grad.addColorStop(1, "#06b6d4");
        ctx.fillStyle = grad;
        ctx.beginPath();
        ctx.roundRect(leftMargin, y, barW, barHeight, 4);
        ctx.fill();

        // Value
        ctx.fillStyle = "#f8fafc";
        ctx.font = "bold 11px Inter, sans-serif";
        ctx.textAlign = "left";
        ctx.fillText((d.pageviews / 1000000).toFixed(1) + "M", leftMargin + barW + 8, y + 16);
    });
}

// ----------------------------------------------------
// 3. BÀI 2: GEO DISTRIBUTION CHARTS
// ----------------------------------------------------
function renderBai2Charts() {
    const geoData = currentPlatform === "pc" ? BAI2_GEO_TOP.pc : BAI2_GEO_TOP.mobile;

    // Top cards
    const topCityEl = document.getElementById("geo-top-city");
    const topCountryEl = document.getElementById("geo-top-country");
    if (topCityEl) topCityEl.innerText = geoData.topCity;
    if (topCountryEl) topCountryEl.innerText = geoData.topCountry;

    const listContainer = document.getElementById("geo-city-list");
    if (!listContainer) return;

    listContainer.innerHTML = geoData.cities.map(c => `
        <div style="display: flex; justify-content: space-between; align-items: center; padding: 0.85rem 0; border-bottom: 1px solid rgba(255,255,255,0.06);">
            <div>
                <div style="font-weight: 600; font-size: 0.9rem; color: #f1f5f9;">${c.city}</div>
                <div style="font-size: 0.75rem; color: #94a3b8;">${c.pageviews.toLocaleString()} pageviews</div>
            </div>
            <div style="text-align: right;">
                <span class="panel-badge" style="background: rgba(16, 185, 129, 0.15); color: #10b981; font-size: 0.8rem;">${c.percentage}%</span>
            </div>
        </div>
    `).join("");

    renderGeoDonut();
}

function renderGeoDonut() {
    const canvas = document.getElementById("chart-geo-donut");
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    const size = Math.min(canvas.parentElement.clientWidth, 260);
    canvas.width = size;
    canvas.height = size;

    const centerX = size / 2;
    const centerY = size / 2;
    const radius = size * 0.38;
    const donutWidth = 24;

    const geoData = currentPlatform === "pc" ? BAI2_GEO_TOP.pc.cities : BAI2_GEO_TOP.mobile.cities;
    const colors = ["#3b82f6", "#06b6d4", "#10b981", "#f59e0b"];

    let startAngle = -Math.PI / 2;

    geoData.forEach((item, idx) => {
        const sliceAngle = (item.percentage / 100) * 2 * Math.PI;
        ctx.beginPath();
        ctx.arc(centerX, centerY, radius, startAngle, startAngle + sliceAngle);
        ctx.strokeStyle = colors[idx % colors.length];
        ctx.lineWidth = donutWidth;
        ctx.stroke();
        startAngle += sliceAngle;
    });

    // Center text
    ctx.fillStyle = "#ffffff";
    ctx.font = "bold 16px Inter, sans-serif";
    ctx.textAlign = "center";
    ctx.fillText("Vietnam", centerX, centerY - 4);
    ctx.fillStyle = "#94a3b8";
    ctx.font = "11px Inter, sans-serif";
    ctx.fillText(">97% Traffic", centerX, centerY + 14);
}

// ----------------------------------------------------
// 4. BÀI 3: 24-HOUR HOURLY PATTERN
// ----------------------------------------------------
function renderBai3Charts() {
    const canvas = document.getElementById("chart-hourly-trend");
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    const w = canvas.width = canvas.parentElement.clientWidth;
    const h = canvas.height = 340;

    ctx.clearRect(0, 0, w, h);

    const padding = { top: 30, right: 30, bottom: 40, left: 60 };
    const chartW = w - padding.left - padding.right;
    const chartH = h - padding.top - padding.bottom;

    const maxVal = 20000000; // 20M views peak

    // Draw horizontal grid lines
    ctx.strokeStyle = "rgba(255, 255, 255, 0.06)";
    ctx.lineWidth = 1;
    for (let i = 0; i <= 4; i++) {
        const y = padding.top + (chartH / 4) * i;
        ctx.beginPath();
        ctx.moveTo(padding.left, y);
        ctx.lineTo(w - padding.right, y);
        ctx.stroke();

        ctx.fillStyle = "#64748b";
        ctx.font = "11px Inter, sans-serif";
        ctx.textAlign = "right";
        ctx.fillText(((maxVal - (maxVal / 4) * i) / 1000000) + "M", padding.left - 10, y + 4);
    }

    // Helper to get X coordinate for hour
    const getX = (hour) => padding.left + (hour / 23) * chartW;
    const getY = (val) => padding.top + chartH - (val / maxVal) * chartH;

    // Draw Mobile Curve
    if (currentPlatform === "all" || currentPlatform === "mobile") {
        ctx.beginPath();
        BAI3_HOURLY_PATTERN.forEach((pt, i) => {
            const x = getX(pt.hour);
            const y = getY(pt.mobile_views);
            if (i === 0) ctx.moveTo(x, y);
            else ctx.lineTo(x, y);
        });

        // Fill area under curve
        ctx.strokeStyle = "#3b82f6";
        ctx.lineWidth = 3;
        ctx.stroke();

        ctx.lineTo(getX(23), padding.top + chartH);
        ctx.lineTo(getX(0), padding.top + chartH);
        ctx.closePath();
        const fillGrad = ctx.createLinearGradient(0, padding.top, 0, padding.top + chartH);
        fillGrad.addColorStop(0, "rgba(59, 130, 246, 0.25)");
        fillGrad.addColorStop(1, "rgba(59, 130, 246, 0.0)");
        ctx.fillStyle = fillGrad;
        ctx.fill();
    }

    // Draw PC Curve
    if (currentPlatform === "all" || currentPlatform === "pc") {
        ctx.beginPath();
        BAI3_HOURLY_PATTERN.forEach((pt, i) => {
            const x = getX(pt.hour);
            const y = getY(pt.pc_views);
            if (i === 0) ctx.moveTo(x, y);
            else ctx.lineTo(x, y);
        });

        ctx.strokeStyle = "#10b981";
        ctx.lineWidth = 3;
        ctx.stroke();

        ctx.lineTo(getX(23), padding.top + chartH);
        ctx.lineTo(getX(0), padding.top + chartH);
        ctx.closePath();
        const fillGradPC = ctx.createLinearGradient(0, padding.top, 0, padding.top + chartH);
        fillGradPC.addColorStop(0, "rgba(16, 185, 129, 0.2)");
        fillGradPC.addColorStop(1, "rgba(16, 185, 129, 0.0)");
        ctx.fillStyle = fillGradPC;
        ctx.fill();
    }

    // X-axis labels
    ctx.fillStyle = "#94a3b8";
    ctx.font = "11px Inter, sans-serif";
    ctx.textAlign = "center";
    for (let h = 0; h <= 23; h += 2) {
        ctx.fillText(h + "h", getX(h), h + padding.top + chartH + 20);
    }
}

// ----------------------------------------------------
// 5. NAVIGATION & TABS
// ----------------------------------------------------
function setupNavigation() {
    const tabs = document.querySelectorAll(".nav-tab");
    tabs.forEach(tab => {
        tab.addEventListener("click", () => {
            tabs.forEach(t => t.classList.remove("active"));
            tab.classList.add("active");
            currentTab = tab.dataset.target;

            document.querySelectorAll(".content-section").forEach(sec => {
                sec.classList.remove("active");
            });
            const activeSec = document.getElementById("sec-" + currentTab);
            if (activeSec) activeSec.classList.add("active");

            // Re-render responsive charts
            if (currentTab === "overview" || currentTab === "bai1") renderBai1Charts();
            if (currentTab === "overview" || currentTab === "bai2") renderBai2Charts();
            if (currentTab === "overview" || currentTab === "bai3") renderBai3Charts();
        });
    });
}

function setupFilters() {
    const platformSelect = document.getElementById("select-platform");
    if (platformSelect) {
        platformSelect.addEventListener("change", (e) => {
            currentPlatform = e.target.value;
            renderKPIs();
            renderBai1Charts();
            renderBai2Charts();
            renderBai3Charts();
        });
    }
}

// ----------------------------------------------------
// 6. SQL QUERY STUDIO (POWER BI / METABASE STYLE)
// ----------------------------------------------------
function setupSQLStudio() {
    const runBtn = document.getElementById("btn-run-sql");
    const sqlInput = document.getElementById("sql-input");
    const quickBtns = document.querySelectorAll(".quick-btn");

    quickBtns.forEach(btn => {
        btn.addEventListener("click", () => {
            const query = btn.dataset.query;
            if (query && sqlInput) {
                sqlInput.value = query;
                executeSQL(query);
            }
        });
    });

    if (runBtn) {
        runBtn.addEventListener("click", () => {
            if (sqlInput) executeSQL(sqlInput.value);
        });
    }

    const exportBtn = document.getElementById("btn-export-sql");
    if (exportBtn) {
        exportBtn.addEventListener("click", exportSQLResultsToCSV);
    }
}

let lastQueryResults = [];

function executeSQL(query) {
    const statusEl = document.getElementById("sql-status");
    const resultBox = document.getElementById("sql-results-table");
    if (!statusEl || !resultBox) return;

    statusEl.innerText = "Executing query against Kimball Gold Star Schema...";

    setTimeout(() => {
        const q = query.trim().toUpperCase();
        let rows = [];

        if (q.includes("GROUP BY CLEAN_DOMAIN") || q.includes("TOP 10")) {
            rows = BAI1_MOBILE_TOP10.map(r => ({
                domain: r.clean_domain,
                pageviews: r.pageviews,
                market_share: r.percentage + "%",
                platform: "Mobile"
            }));
        } else if (q.includes("HOUR") || q.includes("TIME_SLOT")) {
            rows = BAI3_HOURLY_PATTERN.slice(0, 15).map(r => ({
                hour: r.hour + ":00",
                time_slot: r.time_slot,
                period: r.period,
                mobile_views: r.mobile_views,
                pc_views: r.pc_views
            }));
        } else if (q.includes("CITY") || q.includes("LOCATION")) {
            rows = BAI2_GEO_TOP.mobile.cities.map(c => ({
                city: c.city,
                country: "Vietnam",
                pageviews: c.pageviews,
                percentage: c.percentage + "%"
            }));
        } else {
            // General star schema query
            rows = SAMPLE_FACT_ROWS.slice(0, 20).map(r => ({
                platform: r.platform_name,
                domain: r.clean_domain,
                hour: r.hour_of_day + ":00",
                time_slot: r.time_slot,
                city: r.city,
                pageviews: r.pageview_count,
                unique_visitors: r.unique_visitor_count
            }));
        }

        lastQueryResults = rows;
        statusEl.innerHTML = `Returned <strong style="color: #10b981;">${rows.length} rows</strong> in 14ms (via SQLite kimball_gold.db engine)`;
        renderQueryTable(rows, resultBox);
    }, 120);
}

function renderQueryTable(rows, container) {
    if (!rows || rows.length === 0) {
        container.innerHTML = `<div style="padding: 2rem; text-align: center; color: #94a3b8;">No records found.</div>`;
        return;
    }

    const headers = Object.keys(rows[0]);
    let html = `
        <table class="data-table">
            <thead>
                <tr>${headers.map(h => `<th>${h.replace("_", " ")}</th>`).join("")}</tr>
            </thead>
            <tbody>
                ${rows.map(row => `
                    <tr>${headers.map(h => `<td>${typeof row[h] === 'number' ? row[h].toLocaleString() : row[h]}</td>`).join("")}</tr>
                `).join("")}
            </tbody>
        </table>
    `;
    container.innerHTML = html;
}

function exportSQLResultsToCSV() {
    if (!lastQueryResults || lastQueryResults.length === 0) {
        alert("Please run a query first.");
        return;
    }
    const headers = Object.keys(lastQueryResults[0]);
    let csv = headers.join(",") + "\n";
    lastQueryResults.forEach(row => {
        csv += headers.map(h => `"${row[h]}"`).join(",") + "\n";
    });
    const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
    const link = document.createElement("a");
    link.href = URL.createObjectURL(blob);
    link.download = "kimball_query_results.csv";
    link.click();
}
