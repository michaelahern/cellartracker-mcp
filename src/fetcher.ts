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

export async function fetchBottles(username: string, password: string): Promise<Record<string, unknown>[]> {
    const url = `https://www.cellartracker.com/xlquery.asp?User=${encodeURIComponent(username)}&Password=${encodeURIComponent(password)}&Format=tab&Table=Inventory`;
    const response = await fetch(url);

    if (!response.ok) {
        throw new Error(`Failed to fetch data from CellarTracker: ${response.status} ${response.statusText}`);
    }

    const responseText = await response.text();
    const { data } = Papa.parse<Record<string, unknown>>(responseText, {
        delimiter: '\t',
        header: true,
        skipEmptyLines: true,
        dynamicTyping: true
    });

    return data.map((row) => {
        const out: Record<string, unknown> = {};
        for (const [key, val] of Object.entries(row as Record<string, unknown>)) {
            if (!COLUMNS_BOTTLES.has(key)) continue;
            const newKey = COLUMN_RENAMES[key] ?? key;
            out[newKey] = val;
        }
        return out;
    });
}

export async function fetchWines(username: string, password: string): Promise<Record<string, unknown>[]> {
    const url = `https://www.cellartracker.com/xlquery.asp?User=${encodeURIComponent(username)}&Password=${encodeURIComponent(password)}&Format=tab&Table=List`;
    const response = await fetch(url);

    if (!response.ok) {
        throw new Error(`Failed to fetch data from CellarTracker: ${response.status} ${response.statusText}`);
    }

    const responseText = await response.text();
    const { data } = Papa.parse<Record<string, unknown>>(responseText, {
        delimiter: '\t',
        header: true,
        skipEmptyLines: true,
        dynamicTyping: true
    });

    return data.map((row) => {
        const out: Record<string, unknown> = {};
        for (const [key, val] of Object.entries(row as Record<string, unknown>)) {
            if (!COLUMNS_WINES.has(key)) continue;
            const newKey = COLUMN_RENAMES[key] ?? key;
            out[newKey] = val;
        }
        return out;
    });
}
