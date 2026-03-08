export async function initSchema(db: D1Database) {
    await db.prepare(
        'CREATE TABLE IF NOT EXISTS wines (iWine TEXT PRIMARY KEY, Producer TEXT, Wine TEXT, Vintage REAL, Varietal TEXT, Location TEXT, Bin TEXT, Quantity REAL, Size TEXT, Price REAL, Valuation REAL, Currency TEXT, Country TEXT, Region TEXT, SubRegion TEXT, Appellation TEXT, Type TEXT, Color TEXT, Category TEXT, Designation TEXT, Vineyard TEXT, StoreName TEXT, PurchaseDate TEXT, WA REAL, WS REAL, VM REAL, JR TEXT, JD REAL, CT REAL, MY REAL, BeginConsume REAL, EndConsume REAL)'
    ).run();
}

export async function truncateAndInsert(db: D1Database, rows: Record<string, unknown>[]) {
    const columns = [
        'iWine', 'Producer', 'Wine', 'Vintage', 'Varietal', 'Location', 'Bin',
        'Quantity', 'Size', 'Price', 'Valuation', 'Currency', 'Country', 'Region',
        'SubRegion', 'Appellation', 'Type', 'Color', 'Category', 'Designation',
        'Vineyard', 'StoreName', 'PurchaseDate', 'WA', 'WS', 'VM', 'JR', 'JD',
        'CT', 'MY', 'BeginConsume', 'EndConsume'
    ];
    const placeholders = columns.map(() => '?').join(', ');
    const insertSQL = `INSERT OR REPLACE INTO wines (${columns.join(', ')}) VALUES (${placeholders})`;

    const statements: D1PreparedStatement[] = [
        db.prepare('DELETE FROM wines')
    ];

    for (const row of rows) {
        const values = columns.map((col) => {
            const val = row[col];
            return val === '' || val === undefined ? null : val;
        });
        statements.push(db.prepare(insertSQL).bind(...values));
    }

    // D1 batch() supports up to 100 statements per call
    const BATCH_SIZE = 100;
    for (let i = 0; i < statements.length; i += BATCH_SIZE) {
        await db.batch(statements.slice(i, i + BATCH_SIZE));
    }
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
        conditions.push('Location LIKE ?');
        params.push(`%${filters.location}%`);
    }
    if (filters.min_score !== undefined) {
        conditions.push('(CT >= ? OR WA >= ? OR WS >= ? OR VM >= ? OR JD >= ? OR MY >= ?)');
        params.push(filters.min_score, filters.min_score, filters.min_score, filters.min_score, filters.min_score, filters.min_score);
    }

    const where = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
    const sql = `SELECT * FROM wines ${where} ORDER BY Producer, Vintage LIMIT 50`;

    return db.prepare(sql).bind(...params).all();
}

export async function getCellarStats(db: D1Database) {
    const results = await db.batch<Record<string, unknown>>([
        db.prepare(`
            SELECT
                COALESCE(SUM(Quantity), 0) AS total_bottles,
                COALESCE(SUM(Valuation), 0) AS total_value,
                COUNT(DISTINCT iWine) AS unique_wines
            FROM wines
            WHERE Quantity > 0
        `),
        db.prepare(`
            SELECT Varietal, SUM(Quantity) AS bottle_count
            FROM wines
            WHERE Quantity > 0 AND Varietal IS NOT NULL AND Varietal != ''
            GROUP BY Varietal
            ORDER BY bottle_count DESC
            LIMIT 10
        `),
        db.prepare(`
            SELECT Producer, SUM(Quantity) AS bottle_count
            FROM wines
            WHERE Quantity > 0 AND Producer IS NOT NULL AND Producer != ''
            GROUP BY Producer
            ORDER BY bottle_count DESC
            LIMIT 10
        `),
        db.prepare(`
            SELECT COUNT(*) AS count
            FROM wines
            WHERE Quantity > 0
                AND BeginConsume IS NOT NULL
                AND EndConsume IS NOT NULL
                AND BeginConsume <= ?
                AND EndConsume >= ?
        `).bind(new Date().getFullYear(), new Date().getFullYear())
    ]);

    const totals = results[0] ?? { results: [] };
    const varietals = results[1] ?? { results: [] };
    const producers = results[2] ?? { results: [] };
    const drinkingWindow = results[3] ?? { results: [] };

    return {
        totals: totals.results[0],
        top_varietals: varietals.results,
        top_producers: producers.results,
        in_drinking_window: (drinkingWindow.results[0] as Record<string, unknown> | undefined)?.['count'] ?? 0
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

export async function getWinesByLocation(db: D1Database, location: string) {
    return db.prepare(`
        SELECT * FROM wines
        WHERE Quantity > 0 AND Location LIKE ?
        ORDER BY Location, Bin, Producer, Vintage
        LIMIT 200
    `).bind(`%${location}%`).all();
}
