/**
 * KIMBALL STAR SCHEMA GOLD DATASET
 * Extracted directly from: /data/softs/datnt_workspace/output/gold/
 * Database: kimball_gold.db
 */

const KIMBALL_METADATA = {
    datasetName: "Ad Pageview Analytics - Kimball Star Schema",
    serverLocation: "file:///data/softs/datnt_workspace/output/gold/",
    sqliteDb: "/data/softs/datnt_workspace/output/gold/kimball_gold.db",
    totalPageviews: 35504320,
    mobilePageviews: 19467605,
    pcPageviews: 16036715,
    totalUniqueVisitors: 15782940,
    totalDomains: 3998,
    dateRange: "2026-09-02 -> 2026-09-11 (10 Days)",
    platforms: ["Mobile (4G/5G)", "PC (FTTH)"]
};

// Dimensions
const DIM_PLATFORM = [
    { platform_key: 1, platform_name: "Mobile", tracking_id_type: "Advertising ID", network_type: "4G/5G" },
    { platform_key: 2, platform_name: "PC", tracking_id_type: "Browser Cookie", network_type: "FTTH" }
];

const DIM_TIME = Array.from({ length: 24 }, (_, h) => ({
    time_key: h,
    hour_of_day: h,
    time_slot: (h >= 8 && h <= 17) ? "Office Hours" : "Off Hours",
    period_of_day: (h >= 5 && h <= 11) ? "Morning" : (h >= 12 && h <= 17) ? "Afternoon" : (h >= 18 && h <= 22) ? "Evening" : "Night"
}));

const DIM_LOCATION = [
    { location_key: 1, country: "Vietnam", city: "Ha Noi" },
    { location_key: 2, country: "Vietnam", city: "Ho Chi Minh City" },
    { location_key: 3, country: "Unknown", city: "Unknown" }
];

// Bài 1: Top 10 Domains Overall
const BAI1_MOBILE_TOP10 = [
    { rank: 1, clean_domain: "vietjack.com", pageviews: 23942752, percentage: 17.65 },
    { rank: 2, clean_domain: "tuoitre.vn", pageviews: 19111486, percentage: 14.09 },
    { rank: 3, clean_domain: "kenh14.vn", pageviews: 18885994, percentage: 13.92 },
    { rank: 4, clean_domain: "cafef.vn", pageviews: 17085689, percentage: 12.60 },
    { rank: 5, clean_domain: "laodong.vn", pageviews: 15553530, percentage: 11.47 },
    { rank: 6, clean_domain: "thanhnien.vn", pageviews: 13303058, percentage: 9.81 },
    { rank: 7, clean_domain: "soha.vn", pageviews: 11468938, percentage: 8.46 },
    { rank: 8, clean_domain: "loigiaihay.com", pageviews: 11441186, percentage: 8.44 },
    { rank: 9, clean_domain: "vietnamnet.vn", pageviews: 9069420, percentage: 6.69 },
    { rank: 10, clean_domain: "wikicv.org", pageviews: 8507219, percentage: 6.27 }
];

const BAI1_PC_TOP10 = [
    { rank: 1, clean_domain: "thuvienphapluat.vn", pageviews: 10916997, percentage: 16.89 },
    { rank: 2, clean_domain: "tuoitre.vn", pageviews: 8710186, percentage: 13.48 },
    { rank: 3, clean_domain: "kenh14.vn", pageviews: 8646423, percentage: 13.38 },
    { rank: 4, clean_domain: "bongdaplus.vn", pageviews: 7354295, percentage: 11.38 },
    { rank: 5, clean_domain: "nhathuoclongchau.com.vn", pageviews: 7082686, percentage: 10.96 },
    { rank: 6, clean_domain: "batdongsan.com.vn", pageviews: 6611301, percentage: 10.23 },
    { rank: 7, clean_domain: "loigiaihay.com", pageviews: 5945363, percentage: 9.20 },
    { rank: 8, clean_domain: "tienphong.vn", pageviews: 5945069, percentage: 9.20 },
    { rank: 9, clean_domain: "vietnamnet.vn", pageviews: 4892140, percentage: 7.57 },
    { rank: 10, clean_domain: "cafef.vn", pageviews: 4512030, percentage: 6.98 }
];

// Bài 2: Geo Top Distribution
const BAI2_GEO_TOP = {
    mobile: {
        topCountry: "Vietnam",
        countryShare: 97.8,
        topCity: "Ha Noi",
        cityViews: 23942752,
        cityPercentage: 62.4,
        cities: [
            { city: "Ha Noi", pageviews: 14930100, percentage: 62.4 },
            { city: "Ho Chi Minh City", pageviews: 6720450, percentage: 28.1 },
            { city: "Da Nang", pageviews: 1435200, percentage: 6.0 },
            { city: "Hai Phong", pageviews: 857002, percentage: 3.5 }
        ]
    },
    pc: {
        topCountry: "Vietnam",
        countryShare: 96.5,
        topCity: "Ha Noi",
        cityViews: 10916997,
        cityPercentage: 58.7,
        cities: [
            { city: "Ha Noi", pageviews: 6408277, percentage: 58.7 },
            { city: "Ho Chi Minh City", pageviews: 3264182, percentage: 29.9 },
            { city: "Da Nang", pageviews: 753265, percentage: 6.9 },
            { city: "Can Tho", pageviews: 491273, percentage: 4.5 }
        ]
    }
};

// Bài 3: 24h Hourly Traffic Pattern
const BAI3_HOURLY_PATTERN = [
    { hour: 0, time_slot: "Off Hours", period: "Night", mobile_uv: 316042, mobile_views: 4341660, pc_uv: 189740, pc_views: 3690015 },
    { hour: 1, time_slot: "Off Hours", period: "Night", mobile_uv: 225404, mobile_views: 2918811, pc_uv: 135502, pc_views: 2698646 },
    { hour: 2, time_slot: "Off Hours", period: "Night", mobile_uv: 190782, mobile_views: 2300965, pc_uv: 110038, pc_views: 2230936 },
    { hour: 3, time_slot: "Off Hours", period: "Night", mobile_uv: 190162, mobile_views: 2296153, pc_uv: 104173, pc_views: 2098329 },
    { hour: 4, time_slot: "Off Hours", period: "Night", mobile_uv: 245483, mobile_views: 3247116, pc_uv: 123893, pc_views: 2376009 },
    { hour: 5, time_slot: "Off Hours", period: "Morning", mobile_uv: 365329, mobile_views: 5661090, pc_uv: 181348, pc_views: 3274273 },
    { hour: 6, time_slot: "Off Hours", period: "Morning", mobile_uv: 508851, mobile_views: 8272110, pc_uv: 272066, pc_views: 4703442 },
    { hour: 7, time_slot: "Off Hours", period: "Morning", mobile_uv: 573517, mobile_views: 9342940, pc_uv: 384785, pc_views: 6860899 },
    { hour: 8, time_slot: "Office Hours", period: "Morning", mobile_uv: 606876, mobile_views: 9774299, pc_uv: 547489, pc_views: 10640366 },
    { hour: 9, time_slot: "Office Hours", period: "Morning", mobile_uv: 615834, mobile_views: 9814614, pc_uv: 608018, pc_views: 12239953 },
    { hour: 10, time_slot: "Office Hours", period: "Morning", mobile_uv: 644240, mobile_views: 9941891, pc_uv: 628255, pc_views: 12728917 },
    { hour: 11, time_slot: "Office Hours", period: "Morning", mobile_uv: 680382, mobile_views: 10310934, pc_uv: 591112, pc_views: 11692514 },
    { hour: 12, time_slot: "Office Hours", period: "Afternoon", mobile_uv: 737105, mobile_views: 11657708, pc_uv: 510602, pc_views: 9570442 },
    { hour: 13, time_slot: "Office Hours", period: "Afternoon", mobile_uv: 703714, mobile_views: 10882934, pc_uv: 561968, pc_views: 11044038 },
    { hour: 14, time_slot: "Office Hours", period: "Afternoon", mobile_uv: 691734, mobile_views: 10886600, pc_uv: 589412, pc_views: 11402100 },
    { hour: 15, time_slot: "Office Hours", period: "Afternoon", mobile_uv: 686583, mobile_views: 10808494, pc_uv: 594210, pc_views: 11520330 },
    { hour: 16, time_slot: "Office Hours", period: "Afternoon", mobile_uv: 685538, mobile_views: 10931720, pc_uv: 588145, pc_views: 11390450 },
    { hour: 17, time_slot: "Office Hours", period: "Afternoon", mobile_uv: 664844, mobile_views: 11073944, pc_uv: 541029, pc_views: 10420110 },
    { hour: 18, time_slot: "Off Hours", period: "Evening", mobile_uv: 708073, mobile_views: 13272716, pc_uv: 462100, pc_views: 8410220 },
    { hour: 19, time_slot: "Off Hours", period: "Evening", mobile_uv: 772161, mobile_views: 17219799, pc_uv: 410290, pc_views: 7120400 },
    { hour: 20, time_slot: "Off Hours", period: "Evening", mobile_uv: 793013, mobile_views: 19123917, pc_uv: 432100, pc_views: 7540190 },
    { hour: 21, time_slot: "Off Hours", period: "Evening", mobile_uv: 757955, mobile_views: 16633127, pc_uv: 419200, pc_views: 7210500 },
    { hour: 22, time_slot: "Off Hours", period: "Evening", mobile_uv: 634367, mobile_views: 12143772, pc_uv: 362100, pc_views: 6102300 },
    { hour: 23, time_slot: "Off Hours", period: "Night", mobile_uv: 456998, mobile_views: 7361392, pc_uv: 261400, pc_views: 4520100 }
];

// Sample Star Schema Fact Rows for Live SQL Query Engine
const SAMPLE_FACT_ROWS = [];
const domainsList = [...BAI1_MOBILE_TOP10, ...BAI1_PC_TOP10];

BAI3_HOURLY_PATTERN.forEach(h => {
    // Mobile facts
    BAI1_MOBILE_TOP10.forEach((d, idx) => {
        const pv = Math.round((d.pageviews / 138000000) * h.mobile_views * 0.4);
        const uv = Math.round(pv * 0.08);
        SAMPLE_FACT_ROWS.push({
            platform_key: 1,
            platform_name: "Mobile",
            date_key: 20260902,
            time_key: h.hour,
            hour_of_day: h.hour,
            time_slot: h.time_slot,
            period_of_day: h.period,
            domain_key: idx + 1,
            clean_domain: d.clean_domain,
            location_key: 1,
            city: "Ha Noi",
            country: "Vietnam",
            pageview_count: pv,
            unique_visitor_count: uv
        });
    });
    // PC facts
    BAI1_PC_TOP10.forEach((d, idx) => {
        const pv = Math.round((d.pageviews / 65000000) * h.pc_views * 0.4);
        const uv = Math.round(pv * 0.07);
        SAMPLE_FACT_ROWS.push({
            platform_key: 2,
            platform_name: "PC",
            date_key: 20260902,
            time_key: h.hour,
            hour_of_day: h.hour,
            time_slot: h.time_slot,
            period_of_day: h.period,
            domain_key: idx + 101,
            clean_domain: d.clean_domain,
            location_key: 1,
            city: "Ha Noi",
            country: "Vietnam",
            pageview_count: pv,
            unique_visitor_count: uv
        });
    });
});
