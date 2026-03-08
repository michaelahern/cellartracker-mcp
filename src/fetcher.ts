import Papa from 'papaparse';

const COLUMN_RENAMES: Record<string, string> = { RR: 'JD', AG: 'VM' };

const COLUMNS = new Set([
    'iWine', 'Producer', 'Wine', 'Vintage', 'Varietal', 'Location', 'Bin',
    'Quantity', 'Size', 'Price', 'Valuation', 'Currency', 'Country', 'Region',
    'SubRegion', 'Appellation', 'Type', 'Color', 'Category', 'Designation',
    'Vineyard', 'StoreName', 'PurchaseDate', 'WA', 'WS', 'AG', 'JR', 'RR',
    'CT', 'MY', 'BeginConsume', 'EndConsume'
]);

export async function fetchInventory(username: string, password: string): Promise<Record<string, unknown>[]> {
    const url = `https://www.cellartracker.com/xlquery.asp?User=${encodeURIComponent(username)}&Password=${encodeURIComponent(password)}&Format=tab&Table=Inventory`;
    const response = await fetch(url);

    if (!response.ok) {
        throw new Error(`Failed to fetch data from CellarTracker: ${response.status} ${response.statusText}`);
    }

    const responseText = await response.text();
    const { data } = Papa.parse<Record<string, string>>(responseText, {
        delimiter: '\t',
        header: true,
        skipEmptyLines: true
    });

    return data.map((row) => {
        const out: Record<string, unknown> = {};
        for (const [key, val] of Object.entries(row)) {
            if (!COLUMNS.has(key)) continue;
            const newKey = COLUMN_RENAMES[key] ?? key;
            out[newKey] = val;
        }
        return out;
    });
}
