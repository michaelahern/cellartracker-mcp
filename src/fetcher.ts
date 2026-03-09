import Papa from 'papaparse';

const WINES_COLUMNS = new Set([
    'iWine', 'Quantity', 'Pending',
    'Size', 'Vintage', 'Wine', 'Country', 'Region', 'SubRegion', 'Appellation',
    'Producer', 'Type', 'Color', 'Category', 'Varietal', 'Designation', 'Vineyard',
    'WA', 'AG', 'RR', 'CT', 'MY', 'BeginConsume', 'EndConsume'
]);

const BOTTLE_COLUMNS = new Set([
    'Barcode', 'iWine', 'BottleState', 'Location', 'Bin', 'Store', 'BottleCost', 'BottleCostCurrency', 'PurchaseDate', 'DeliveryDate', 'ConsumptionDate',
    'BottleSize', 'Vintage', 'Wine', 'Country', 'Region', 'SubRegion', 'Appellation',
    'Producer', 'Type', 'Varietal', 'Designation', 'Vineyard',
    'BeginConsume', 'EndConsume'
]);

const REVIEW_COLUMNS = new Set([
    'iReview', 'iWine', 'Publication', 'ReviewDate', 'Reviewer',
    'Score', 'ReviewText', 'ReviewURL', 'BeginConsume', 'EndConsume'
]);

const COLUMN_RENAMES: Record<string, string> = { RR: 'JD', AG: 'VM' };
const DATE_COLUMNS = new Set(['ReviewDate', 'PurchaseDate', 'DeliveryDate', 'ConsumptionDate']);
const SCORE_COLUMNS = new Set(['WA', 'AG', 'RR', 'CT', 'MY', 'Score']);

function decodeHtmlEntities(text: string): string {
    if (!text.includes('&')) return text;

    const named_html_entities: Record<string, string> = {
        '&amp;': '&', '&lt;': '<', '&gt;': '>', '&quot;': '"', '&apos;': '\''
    };

    return text
        .replace(/&#(\d+);/g, (_, code) => String.fromCodePoint(Number(code)))
        .replace(/&#x([0-9a-fA-F]+);/g, (_, hex) => String.fromCodePoint(parseInt(hex as string, 16)))
        .replace(/&(?:amp|lt|gt|quot|apos);/g, m => named_html_entities[m] ?? m);
}

function normalizeDate(val: unknown): string | null {
    if (typeof val !== 'string' || val === '') return null;
    const parts = val.split('/');
    if (parts.length !== 3) return val;
    const [month = '', day = '', year = ''] = parts;
    return `${year}-${month.padStart(2, '0')}-${day.padStart(2, '0')}`;
}

function normalizeScore(val: unknown): number | null {
    if (typeof val === 'number') return Number.isFinite(val) ? Math.round(val) : null;
    if (typeof val !== 'string') return null;

    // Strip parentheses, e.g. "(97-99)" -> "97-99"
    const cleaned = val.replace(/[()]/g, '').trim();
    if (cleaned === '') return null;

    // "96-98" -> take first number; "98+" -> parseFloat ignores trailing +
    const parsed = parseFloat(cleaned);
    return Number.isFinite(parsed) ? Math.round(parsed) : null;
}

async function fetchCellarTrackerTable(username: string, password: string, table: string): Promise<string> {
    const url = `https://www.cellartracker.com/xlquery.asp?User=${encodeURIComponent(username)}&Password=${encodeURIComponent(password)}&Format=tab&Table=${encodeURIComponent(table)}`;
    const response = await fetch(url);

    if (!response.ok) {
        throw new Error(`Failed to fetch data from CellarTracker: ${response.status} ${response.statusText}`);
    }

    const buffer = await response.arrayBuffer();
    const text = new TextDecoder('windows-1252').decode(buffer);
    return decodeHtmlEntities(text);
}

function parseCellarTrackerTable(responseText: string, columns: Set<string>): FetchResult {
    const { data, errors } = Papa.parse<Record<string, unknown>>(responseText, {
        delimiter: '\t',
        quoteChar: '\0',
        header: true,
        dynamicTyping: true,
        skipEmptyLines: true
    });

    const rows = data.map((row) => {
        const out: Record<string, unknown> = {};
        for (const [key, val] of Object.entries(row as Record<string, unknown>)) {
            if (!columns.has(key)) continue;
            const newKey = COLUMN_RENAMES[key] ?? key;
            const normalized = SCORE_COLUMNS.has(key) ? normalizeScore(val) : val === 'Unknown' ? null : val;
            out[newKey] = DATE_COLUMNS.has(key) ? normalizeDate(normalized) : normalized;
        }
        return out;
    });

    return {
        rows,
        diagnostics: {
            responseBytes: responseText.length,
            parsedRows: data.length,
            parseErrors: errors.length,
            firstError: errors[0] ? `Row ${String(errors[0].row)}: ${errors[0].message}` : undefined
        }
    };
}

export interface FetchResult {
    rows: Record<string, unknown>[];
    diagnostics: {
        responseBytes: number;
        parsedRows: number;
        parseErrors: number;
        firstError?: string | undefined;
    };
}

export async function fetchWines(username: string, password: string): Promise<FetchResult> {
    const table = await fetchCellarTrackerTable(username, password, 'List');
    return parseCellarTrackerTable(table, WINES_COLUMNS);
}

export async function fetchBottles(username: string, password: string): Promise<FetchResult> {
    const table = await fetchCellarTrackerTable(username, password, 'Bottles');
    return parseCellarTrackerTable(table, BOTTLE_COLUMNS);
}

export async function fetchReviews(username: string, password: string): Promise<FetchResult> {
    const table = await fetchCellarTrackerTable(username, password, 'ProReview');
    return parseCellarTrackerTable(table, REVIEW_COLUMNS);
}
