export async function initSchema(db: D1Database) {
    await db.batch([
        db.prepare(
            'CREATE TABLE IF NOT EXISTS bottles (Barcode INTEGER PRIMARY KEY, iWine INTEGER, Location TEXT, Bin TEXT, StoreName TEXT, PurchaseDate TEXT, Size TEXT, Vintage INTEGER, Wine TEXT, Country TEXT, Region TEXT, SubRegion TEXT, Appellation TEXT, Producer TEXT, Type TEXT, Color TEXT, Category TEXT, Varietal TEXT, Designation TEXT, Vineyard TEXT, WA INTEGER, VM INTEGER, JD INTEGER, CT INTEGER, MY INTEGER, BeginConsume INTEGER, EndConsume INTEGER)'
        ),
        db.prepare(
            'CREATE TABLE IF NOT EXISTS wines (iWine INTEGER PRIMARY KEY, Quantity INTEGER, Pending INTEGER, Size TEXT, Vintage INTEGER, Wine TEXT, Country TEXT, Region TEXT, SubRegion TEXT, Appellation TEXT, Producer TEXT, Type TEXT, Color TEXT, Category TEXT, Varietal TEXT, Designation TEXT, Vineyard TEXT, WA INTEGER, VM INTEGER, JD INTEGER, CT INTEGER, MY INTEGER, BeginConsume INTEGER, EndConsume INTEGER)'
        )
    ]);
}

const BOTTLE_COLUMNS = [
    'Barcode', 'iWine', 'Location', 'Bin', 'StoreName', 'PurchaseDate',
    'Size', 'Vintage', 'Wine', 'Country', 'Region', 'SubRegion', 'Appellation',
    'Producer', 'Type', 'Color', 'Category', 'Varietal', 'Designation', 'Vineyard',
    'WA', 'VM', 'JD', 'CT', 'MY', 'BeginConsume', 'EndConsume'
];

const WINE_COLUMNS = [
    'iWine', 'Quantity', 'Pending',
    'Size', 'Vintage', 'Wine', 'Country', 'Region', 'SubRegion', 'Appellation',
    'Producer', 'Type', 'Color', 'Category', 'Varietal', 'Designation', 'Vineyard',
    'WA', 'VM', 'JD', 'CT', 'MY', 'BeginConsume', 'EndConsume'
];

async function truncateAndInsert(db: D1Database, table: string, columns: string[], rows: Record<string, unknown>[]) {
    const placeholders = columns.map(() => '?').join(', ');
    const insertSQL = `INSERT OR REPLACE INTO ${table} (${columns.join(', ')}) VALUES (${placeholders})`;

    await db.prepare(`DELETE FROM ${table}`).run();

    const statements: D1PreparedStatement[] = [];
    for (const row of rows) {
        const values = columns.map((col) => {
            const val = row[col];
            return val === '' || val === undefined ? null : val;
        });
        statements.push(db.prepare(insertSQL).bind(...values));
    }

    const BATCH_SIZE = 20;
    for (let i = 0; i < statements.length; i += BATCH_SIZE) {
        await db.batch(statements.slice(i, i + BATCH_SIZE));
    }
}

export async function truncateAndInsertBottles(db: D1Database, rows: Record<string, unknown>[]) {
    await truncateAndInsert(db, 'bottles', BOTTLE_COLUMNS, rows);
}

export async function truncateAndInsertWines(db: D1Database, rows: Record<string, unknown>[]) {
    await truncateAndInsert(db, 'wines', WINE_COLUMNS, rows);
}

export interface SearchFilters {
    producer?: string | undefined;
    varietal?: string | undefined;
    vintage_min?: number | undefined;
    vintage_max?: number | undefined;
    location?: string | undefined;
    min_score?: number | undefined;
    in_stock_only?: boolean | undefined;
}

export async function searchWines(db: D1Database, filters: SearchFilters) {
    const conditions: string[] = [];
    const params: unknown[] = [];

    if (filters.in_stock_only !== false) {
        conditions.push('Quantity > 0');
    }
    if (filters.producer) {
        conditions.push('Producer LIKE ?');
        params.push(`%${filters.producer}%`);
    }
    if (filters.varietal) {
        conditions.push('Varietal LIKE ?');
        params.push(`%${filters.varietal}%`);
    }
    if (filters.vintage_min !== undefined) {
        conditions.push('Vintage >= ?');
        params.push(filters.vintage_min);
    }
    if (filters.vintage_max !== undefined) {
        conditions.push('Vintage <= ?');
        params.push(filters.vintage_max);
    }
    if (filters.location) {
        // Location lives on bottles; join to filter wines by bottle location
        conditions.push('iWine IN (SELECT DISTINCT iWine FROM bottles WHERE Location LIKE ?)');
        params.push(`%${filters.location}%`);
    }
    if (filters.min_score !== undefined) {
        conditions.push('(CT >= ? OR WA >= ? OR VM >= ? OR JD >= ? OR MY >= ?)');
        params.push(filters.min_score, filters.min_score, filters.min_score, filters.min_score, filters.min_score);
    }

    const where = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
    const sql = `SELECT * FROM wines ${where} ORDER BY Producer, Vintage LIMIT 50`;

    return db.prepare(sql).bind(...params).all();
}

export async function getCellarStats(db: D1Database) {
    const results = await db.batch<Record<string, unknown>>([
        db.prepare(`
            SELECT
                COALESCE(SUM(Quantity), 0) AS bottles_in_cellar,
                COALESCE(SUM(Pending), 0) AS bottles_pending_delivery,
                COALESCE(SUM(Quantity), 0) + COALESCE(SUM(Pending), 0) AS bottles_total,
                COUNT(*) AS unique_wines
            FROM wines
        `),
        db.prepare(`
            SELECT Location AS location, COUNT(*) AS bottles_total
            FROM bottles
            WHERE Location IS NOT NULL AND Location != ''
            GROUP BY Location
            ORDER BY bottles_total DESC
            LIMIT 100
        `),
        db.prepare(`
            SELECT
                CASE
                    WHEN EndConsume < CAST(strftime('%Y', 'now') AS INTEGER) THEN 'matured'
                    WHEN BeginConsume <= CAST(strftime('%Y', 'now') AS INTEGER) AND EndConsume >= CAST(strftime('%Y', 'now') AS INTEGER) THEN 'now'
                    WHEN BeginConsume > CAST(strftime('%Y', 'now') AS INTEGER) THEN 'starting ' || CAST(BeginConsume AS TEXT)
                    ELSE 'unknown'
                END AS window,
                COALESCE(SUM(Quantity), 0) AS bottles_in_cellar,
                COALESCE(SUM(Quantity), 0) + COALESCE(SUM(Pending), 0) AS bottles_total
            FROM wines
            WHERE BeginConsume IS NOT NULL AND EndConsume IS NOT NULL
            GROUP BY window
            ORDER BY window ASC
        `),
        db.prepare(`
            SELECT Type AS type, COALESCE(SUM(Quantity), 0) AS bottles_in_cellar, COALESCE(SUM(Quantity), 0) + COALESCE(SUM(Pending), 0) AS bottles_total
            FROM wines
            WHERE Type IS NOT NULL AND Type != '' AND Type != 'Unknown'
            GROUP BY Type
            ORDER BY bottles_total DESC
            LIMIT 20
        `),
        db.prepare(`
            SELECT Varietal AS varietal, COALESCE(SUM(Quantity), 0) AS bottles_in_cellar, COALESCE(SUM(Quantity), 0) + COALESCE(SUM(Pending), 0) AS bottles_total,
                ROUND(AVG(CASE WHEN VM IS NOT NULL THEN VM END), 1) AS avg_score_vm, ROUND(AVG(CASE WHEN JD IS NOT NULL THEN JD END), 1) AS avg_score_jd,
                ROUND(AVG(CASE WHEN WA IS NOT NULL THEN WA END), 1) AS avg_score_wa, ROUND(AVG(CASE WHEN CT IS NOT NULL THEN CT END), 1) AS avg_score_ct
            FROM wines
            WHERE Varietal IS NOT NULL AND Varietal != '' AND Varietal != 'Unknown'
            GROUP BY Varietal
            ORDER BY bottles_total DESC
            LIMIT 20
        `),
        db.prepare(`
            SELECT Producer AS producer, COALESCE(SUM(Quantity), 0) AS bottles_in_cellar, COALESCE(SUM(Quantity), 0) + COALESCE(SUM(Pending), 0) AS bottles_total,
                ROUND(AVG(CASE WHEN VM IS NOT NULL THEN VM END), 1) AS avg_score_vm, ROUND(AVG(CASE WHEN JD IS NOT NULL THEN JD END), 1) AS avg_score_jd,
                ROUND(AVG(CASE WHEN WA IS NOT NULL THEN WA END), 1) AS avg_score_wa, ROUND(AVG(CASE WHEN CT IS NOT NULL THEN CT END), 1) AS avg_score_ct
            FROM wines
            WHERE Producer IS NOT NULL AND Producer != '' AND Producer != 'Unknown'
            GROUP BY Producer
            ORDER BY bottles_total DESC
            LIMIT 20
        `),
        db.prepare(`
            SELECT Country AS country, COALESCE(SUM(Quantity), 0) AS bottles_in_cellar, COALESCE(SUM(Quantity), 0) + COALESCE(SUM(Pending), 0) AS bottles_total
            FROM wines
            WHERE Country IS NOT NULL AND Country != '' AND Country != 'Unknown'
            GROUP BY Country
            ORDER BY bottles_total DESC
            LIMIT 10
        `),
        db.prepare(`
            SELECT Region AS region, Country AS country, COALESCE(SUM(Quantity), 0) AS bottles_in_cellar, COALESCE(SUM(Quantity), 0) + COALESCE(SUM(Pending), 0) AS bottles_total
            FROM wines
            WHERE Region IS NOT NULL AND Region != '' AND Region != 'Unknown'
            GROUP BY Region
            ORDER BY bottles_total DESC
            LIMIT 10
        `),
        db.prepare(`
            SELECT SubRegion AS sub_region, Region AS region, Country AS country, COALESCE(SUM(Quantity), 0) AS bottles_in_cellar, COALESCE(SUM(Quantity), 0) + COALESCE(SUM(Pending), 0) AS bottles_total
            FROM wines
            WHERE SubRegion IS NOT NULL AND SubRegion != '' AND SubRegion != 'Unknown'
            GROUP BY SubRegion
            ORDER BY bottles_total DESC
            LIMIT 10
        `),
        db.prepare(`
            SELECT Appellation AS appellation, SubRegion AS sub_region, Region AS region, Country AS country, COALESCE(SUM(Quantity), 0) AS bottles_in_cellar, COALESCE(SUM(Quantity), 0) + COALESCE(SUM(Pending), 0) AS bottles_total
            FROM wines
            WHERE Appellation IS NOT NULL AND Appellation != '' AND Appellation != 'Unknown'
            GROUP BY Appellation
            ORDER BY bottles_total DESC
            LIMIT 10
        `)
    ]);

    const totals = results[0] ?? { results: [] };
    const locations = results[1] ?? { results: [] };
    const drinkingWindows = results[2] ?? { results: [] };
    const types = results[3] ?? { results: [] };
    const varietals = results[4] ?? { results: [] };
    const producers = results[5] ?? { results: [] };
    const countries = results[6] ?? { results: [] };
    const regions = results[7] ?? { results: [] };
    const subRegions = results[8] ?? { results: [] };
    const appellations = results[9] ?? { results: [] };

    return {
        totals: totals.results[0],
        locations: locations.results,
        drinking_window: drinkingWindows.results,
        top_types: types.results,
        top_varietals: varietals.results,
        top_producers: producers.results,
        top_countries: countries.results,
        top_regions: regions.results,
        top_sub_regions: subRegions.results,
        top_appellations: appellations.results
    };
}

export async function getDrinkingWindows(db: D1Database, withinYears: number) {
    const currentYear = new Date().getFullYear();
    const targetYear = currentYear + withinYears;

    return db.prepare(`
        SELECT * FROM wines
        WHERE Quantity > 0
            AND BeginConsume IS NOT NULL
            AND EndConsume IS NOT NULL
            AND BeginConsume <= ?
            AND EndConsume >= ?
        ORDER BY EndConsume ASC
        LIMIT 100
    `).bind(targetYear, currentYear).all();
}

export async function getBottlesByLocation(db: D1Database, location: string) {
    return db.prepare(`
        SELECT * FROM bottles
        WHERE Location LIKE ?
        ORDER BY Location, Bin, Producer, Vintage
        LIMIT 200
    `).bind(`%${location}%`).all();
}
