import Papa from 'papaparse';

const COLUMN_RENAMES: Record<string, string> = { RR: 'JD', AG: 'VM' };

const COLUMNS_BOTTLES = new Set([
    'Barcode', 'iWine', 'Location', 'Bin', 'StoreName', 'PurchaseDate',
    'Size', 'Vintage', 'Wine', 'Country', 'Region', 'SubRegion', 'Appellation',
    'Producer', 'Type', 'Color', 'Category', 'Varietal', 'Designation', 'Vineyard',
    'WA', 'AG', 'RR', 'CT', 'MY', 'BeginConsume', 'EndConsume'
]);

const COLUMNS_WINES = new Set([
    'iWine', 'Quantity', 'Pending',
    'Size', 'Vintage', 'Wine', 'Country', 'Region', 'SubRegion', 'Appellation',
    'Producer', 'Type', 'Color', 'Category', 'Varietal', 'Designation', 'Vineyard',
    'WA', 'AG', 'RR', 'CT', 'MY', 'BeginConsume', 'EndConsume'
]);

export interface FetchResult {
    rows: Record<string, unknown>[];
    diagnostics: {
        responseBytes: number;
        parsedRows: number;
        parseErrors: number;
        firstError?: string | undefined;
    };
}

function parseTSV(responseText: string, columns: Set<string>): FetchResult {
    const { data, errors } = Papa.parse<Record<string, unknown>>(responseText, {
        delimiter: '\t',
        header: true,
        skipEmptyLines: true,
        dynamicTyping: true
    });

    const rows = data.map((row) => {
        const out: Record<string, unknown> = {};
        for (const [key, val] of Object.entries(row as Record<string, unknown>)) {
            if (!columns.has(key)) continue;
            const newKey = COLUMN_RENAMES[key] ?? key;
            out[newKey] = val;
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

async function fetchTSV(username: string, password: string, table: string): Promise<string> {
    const url = `https://www.cellartracker.com/xlquery.asp?User=${encodeURIComponent(username)}&Password=${encodeURIComponent(password)}&Format=tab&Table=${encodeURIComponent(table)}`;
    const response = await fetch(url);

    if (!response.ok) {
        throw new Error(`Failed to fetch data from CellarTracker: ${response.status} ${response.statusText}`);
    }

    return response.text();
}

export async function fetchBottles(username: string, password: string): Promise<FetchResult> {
    const responseText = await fetchTSV(username, password, 'Inventory');
    return parseTSV(responseText, COLUMNS_BOTTLES);
}

export async function fetchWines(username: string, password: string): Promise<FetchResult> {
    const responseText = await fetchTSV(username, password, 'List');
    return parseTSV(responseText, COLUMNS_WINES);
}
